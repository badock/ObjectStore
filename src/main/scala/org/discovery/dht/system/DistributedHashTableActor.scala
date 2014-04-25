package org.discovery.dht.system

/* ============================================================
 * Discovery Project - Distributed Hash Table
 * http://beyondtheclouds.github.io/
 * ============================================================
 * Copyright 2013 Discovery Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============================================================ */

import akka.actor.{ActorRef, ActorLogging, Actor}
import org.discovery.AkkaArc.util.NodeRef
import scala.collection.mutable
import org.discovery.dht.system.DistributedHashTableProtocol._
import scala.concurrent.{Future, ExecutionContext}
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout

case class Version(val key: String, val value: Any, version: Long)

class DistributedHashTableActor(supervisor: NodeRef) extends Actor with ActorLogging {

  /*
  *
  * Data
  *
  * */

  var map: mutable.HashMap[String, Version] = new mutable.HashMap[String, Version]()
  var writeEnabled: Boolean = true

  /*
  *
  * Partition Management
  *
  * */


  val selfRef: NodeRef = NodeRef(supervisor.location, self)
  var partitionMembers: List[NodeRef] = List(selfRef)

  var joinToken: Boolean = true
  var waitingForToken: List[ActorRef] = Nil

  var predecessor: NodeRef = selfRef
  var successor: NodeRef = selfRef

  var shortcuts: List[Shortcut] = Nil
  var writeHistoric: List[Version] = Nil

  //  val id: Long = Random.nextLong()
  val id: Long = supervisor.location.getId

  /*
  *
  * Shortcuts methods
  *
  * */

  val log2 = (x: Double) => scala.math.log(x) / scala.math.log(2)

  val shortcutsIndex: List[Long] = {
    val rawShortcuts = (for (i <- 0 to Math.ceil(log2(Long.MaxValue.toDouble * 2)).toInt) yield {
      val nextId = (id + Math.pow(2, i).toLong)
      nextId
    }).sorted.distinct.toList
    rawShortcuts.filter(s => s > this.id) ::: rawShortcuts.filter(s => s < this.id)
  }

  val shortcutsData: mutable.Map[Long, Option[Shortcut]] = collection.mutable.Map() ++ shortcutsIndex.map({
    shortcut => (shortcut, None).asInstanceOf[(Long, Option[Shortcut])]
  }).toMap

  def updateShortcuts(remoteNode: NodeRef, remoteId: Long, debug: Boolean = false) {

    if(remoteId == this.id) {
      return
    }


    /*
    *
    * Update predecessor
    *
    * */

    val currentPredecessorId = predecessor.location.getId
    if(currentPredecessorId == this.id || isInInterval(remoteId, currentPredecessorId, this.id)) {
        predecessor = remoteNode
    }

    /*
    *
    * Update successor
    *
    * */

    val currentSuccessorId = successor.location.getId
    if(currentSuccessorId == this.id || isInInterval(remoteId, this.id, currentSuccessorId)) {
      successor = remoteNode
    }

     /*
    *
    * Update shortcuts
    *
    * */

     shortcutsData.foreach(keyVal =>

       if(isInInterval(remoteId, keyVal._1, this.id - 1)) {
         keyVal._2 match {
           case Some(shortcut) if(isInInterval(remoteId, keyVal._1, shortcut.node.location.getId)) =>
             shortcutsData.put(keyVal._1, Some(Shortcut(keyVal._1, remoteNode)))
           case None =>
             shortcutsData.put(keyVal._1, Some(Shortcut(keyVal._1, remoteNode)))
           case _ =>
         }
       }
    )
  }

  def closestPrecedingNode(_id: Long): NodeRef = {

    shortcutsIndex.reverse.foreach(index => {
      (index > this.id, _id > index, _id > this.id ,shortcutsData.get(index)) match {
        case (after@true, between@true, afterId@true, Some(Some(shortcut))) if(shortcut.node.location.getId < _id) =>
          return shortcut.node
        case (above@true, between@false, afterId@false, Some(Some(shortcut))) if(_id < 0 && shortcut.node.location.getId > 0) =>
          return shortcut.node
        case (above@true, between@false, afterId@false, Some(Some(shortcut))) if(_id > 0 && shortcut.node.location.getId < _id) =>
          return shortcut.node
        case (above@false, between@true, afterId@false, Some(Some(shortcut))) if(_id <= this.id && shortcut.node.location.getId < _id) =>
          return shortcut.node
        case (a, b, c, d) =>
      }
    })

    selfRef
  }

  /*
  *
  * This method is rather long. See ShortcutsTest.[...]."must contain accurate shortcuts" for
  * explanations.
  *
  * */
  def isInInterval(_id: Long, a: Long, b: Long): Boolean = {

    if(a == b) {
      return _id == a
    }

    (a > 0, b > 0) match {
      case (aIsPositive@true,  bIsPositive@true) =>
        !(a < b && (_id < a || _id > b)) &&            // case 1
        !(a > b && _id > b && _id < a)                 // case 2
      case (aIsNegative@false, bIsPositive@true) =>
        !(_id > b || _id < a)                          // case 3
      case (aIsPositive@true,  bIsNegative@false) =>
        !(_id > b && _id < a)                          // case 4
      case (aIsNegative@false, bIsNegative@false) =>
        !(a > b && _id < a && _id > b) &&              // case 5
        !(a < b && ( _id > b || _id < a))              // case 6
    }
  }

  def printDebug() {
    var s:String = s"Fingers of ${this.id}\n"
    shortcutsIndex.foreach(index =>
      s += s"[${this.id}] $index => ${shortcutsData.get(index)}\n"
    )
    println(s)
  }

  implicit val timeout = Timeout(2 seconds)

  def lookUp(_id: Long, debug: Boolean, origin: Long = 0, already: List[Long] = Nil): Future[Option[NodeRef]] = {

    if(already.contains(this.id)) {
      return Future {
        None
      }
    }

    if(this.id == _id) {
      Future {
        Some(selfRef)
      }
    } else {

      val closest = closestPrecedingNode(_id+1)

      if(closest.location.getId == _id) {
        Future {
          Some(closest)
        }
      } else {
        for {
          successor <- (closest.ref ? LookUp(_id, debug, origin, this.id :: already)).mapTo[Option[NodeRef]]
        } yield {
          successor
        }
      }
    }
  }

  def findSuccessor(_id: Long, debug: Boolean, origin: Long = 0, already: List[Long] = Nil): Future[NodeRef] = {

    if(already.contains(this.id)) {
      return Future {
        selfRef
      }
    }


    val successorId: Long = successor.location.getId

    if((successorId != _id && isInInterval(_id, this.id, successorId)) || _id == this.id) {
      Future {
        successor
      }
    } else {

      var closest = closestPrecedingNode(_id)

      if(closest.location.getId == this.id) {
        closest = successor
      }


      if(closest.location.getId == this.id) {
        Future {
          successor
        }
      } else {
        for {
          successor <- (closest.ref ? FindSuccessor(_id, debug, origin, this.id :: already)).mapTo[NodeRef]
        } yield {
          successor
        }
      }
    }
  }

  /*
 *
 * Shortcuts - stabilization
 *
 * */

  def stabilize(debug: Boolean = false) {
    for {
      predecessorOfSuccessor <- (successor.ref ? GetPredecessor()).mapTo[NodeRef]
    } yield {
      val predecessorOfSuccessorId = predecessorOfSuccessor.location.getId
      val successorId = successor.location.getId

      if(isInInterval(predecessorOfSuccessorId, this.id+1, successorId)) {
        updateShortcuts(predecessorOfSuccessor, predecessorOfSuccessorId)
      }
    }

    if(successor.location.getId != this.id) {
      successor.ref ! Notify(selfRef)
    }
  }

  def notify(remote: NodeRef) {
    val remoteId = remote.location.getId
    val predecessorId = predecessor.location.getId

    if(remoteId != this.id && (predecessorId == this.id || isInInterval(remoteId, predecessorId, this.id -1))) {
      predecessor = remote
    }
  }

  var next: Int = 0
  def fixShortcuts(debug: Boolean = false) {
    val index: Long = shortcutsIndex(next)
    for {
      successor <- findSuccessor(index, debug)
    } yield {
        updateShortcuts(successor, successor.location.getId, debug)
    }
    next = (next + 1) % shortcutsIndex.size
  }

  /*
 *
 * Local - Map methods
 *
 * */


  def get(key: String) =
    map.get(key)

  def put(key: String, value: Version) {
    map.put(key, value)
  }

  def remove(key: String) {
    map.remove(key)
  }

  def fusionLocalMapWithRemoteMap(remoteMap: mutable.HashMap[String, Version]) {
    remoteMap.foreach(tuple2 =>
      if (!map.contains(tuple2._1)) {
        map.put(tuple2._1, tuple2._2)
        log.info(s"$selfRef: received a non existing $tuple2")
      } else {
        (tuple2._2, map.get(tuple2._1).get) match {
          case (Version(key1, value1, version1), Version(key2, value2, version2)) if (version1 > version2) =>
            log.info(s"$selfRef: received a more recent $tuple2")
            map.put(key1, tuple2._2)
          case _ =>
        }
      }
    )
  }

  /*
 *
 * Message reception loop
 *
 * */

  def receive = {

    case Get(key) =>
      sender ! get(key)

    case Write(key, value) =>
      put(key, value)

    case Get(key) =>
      remove(key)

    case GetPartitionMembers() =>
      sender ! partitionMembers

    case GetDhtId() =>
      sender ! id

    case GetNodeRef() =>
      sender ! selfRef

    case FusionKeys(remoteMap) =>
      fusionLocalMapWithRemoteMap(remoteMap)


    /* Messages exchanged during joining phase */

    case MergeThisPartition(partition) =>
      partitionMembers = (partition ::: partitionMembers).distinct
      partition.foreach(m => m.ref ! FusionKeys(map))

    case UpdateShortcuts(remoteNode, remoteId) =>
      updateShortcuts(remoteNode, remoteId)

    case WaitJoinToken() =>
      if (joinToken) {
        joinToken = false
        sender ! true
      } else {
        waitingForToken = sender :: waitingForToken
      }

    case ReleaseJoinToken() =>
      waitingForToken match {
        case first :: xs =>
          joinToken = false
          waitingForToken = xs
          first ! true
        case Nil =>
          joinToken = true
      }

    case GetPredecessor() =>
      sender ! predecessor

    case GetSuccessor() =>
      sender ! successor

    case Notify(remote) =>
      notify(remote)

    /* Node retrieval messages */
    case LookUp(id, bool, origin, already) =>
      val s = sender
      for {
        successor <- lookUp(id, bool, origin, already)
      } yield
        s ! successor


    case FindSuccessor(id, bool, origin, already) =>
      val s = sender
      for {
        successor <- findSuccessor(id, bool, origin, already)
      } yield
        s ! successor

    case Tick() =>
      stabilize()
      fixShortcuts(true)

    case Debug() =>
      printDebug()

    case msg =>
  }

  context.system.scheduler.schedule(100 milliseconds,
    100 milliseconds,
    self,
    Tick())

}
