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

import org.discovery.AkkaArc.util.NodeRef
import scala.concurrent.{ExecutionContext, Await, Future}
import akka.actor.{Props, TypedActor, ActorRef}
import scala.concurrent.duration._
import org.discovery.dht.system.DistributedHashTableProtocol._
import akka.pattern.ask
import akka.util.Timeout
import java.util.concurrent.Executors
import java.security.MessageDigest
import org.discovery.dht.Util

case class Shortcut(val id: Long, node: NodeRef)

trait DistributedHashTableService {

  def consolidatePartition(r: Int)

  def localWrite(key: String, value: Any)

  def localGet(key: String): Future[Option[Version]]

  def put(key: String, value: Any)

  def get(key: String): Future[Option[Version]]

  def synchronize(key: String, value: Option[Version])

  def join(service: DistributedHashTableService)

  def getId(): Future[Long]

  def getNodeRef(): Future[NodeRef]

  def getPartitionMembers(): Future[List[NodeRef]]

  def connect(service: DistributedHashTableService)

  def getPredecessor(): Future[NodeRef]

  def getSuccessor(): Future[NodeRef]

  def lookUp(id: Long, debug: Boolean = false): Future[Option[NodeRef]]

  def findSuccessor(id: Long, debug: Boolean = false): Future[NodeRef]

  def debug()

  def actor: ActorRef
}

abstract class DistributedHashTableServiceImpl(supervisor: NodeRef) extends DistributedHashTableService {

  /*
  *
  * Data
  *
  * */

  val id: Long = supervisor.location.getId
  val self: DistributedHashTableService = this

  /*
  *
  * Data management method
  *
  * */

  implicit val timeout = Timeout(2 seconds)
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  def consolidatePartition(r: Int) {

  }

  def onNodeFailure = (n: NodeRef) => {

  }

  def localWrite(key: String, value: Any) {

    val currentVersionPromise = (actor ? Get(key)).mapTo[Option[Version]]
    val currentVersion = Await.result(currentVersionPromise, 2 seconds)

    val newVersion = currentVersion match {
      case Some(previousVersion) =>
        Version(key, value, previousVersion.version + 1)
      case None =>
        Version(key, value, 0)
    }

    actor ! Write(key, newVersion)
    synchronize(key, Some(newVersion))
  }

  def localGet(key: String): Future[Option[Version]] = {
    (actor ? Get(key)).mapTo[Option[Version]]
  }

  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes)
  }


  def put(key: String, value: Any) {
    val keyId: Long = Util.md5(key)

    val future = for {
      relevantNode <- findSuccessor(keyId)
    } yield {
      val relevantService = new RemoteDistributedHashTableServiceImpl(supervisor, relevantNode.ref)
      relevantService.localWrite(key, value)
    }

    Await.result(future, 2 seconds)
  }

  def get(key: String): Future[Option[Version]] = {
    val keyId: Long = Util.md5(key)

    for {
      relevantNode <- findSuccessor(keyId)
      relevantService <- Future {
        new RemoteDistributedHashTableServiceImpl(supervisor, relevantNode.ref)
      }
      valueOption <- relevantService.localGet(key)
    } yield
      valueOption

  }


  def synchronize(key: String, value: Option[Version]) {
    implicit val timeout = Timeout(2 seconds)

    val partition = Await.result(getPartitionMembers(), 2 seconds)

    /* Collect different versions of the key */
    val versions = partition.map(n => Await.result((n.ref ? Get(key)).mapTo[Option[Version]], 2 seconds))

    /* Resolve potential conflicts */
    val conflictDetected: Boolean = (versions.flatten.map(v => v.version).distinct).size > 1
    if (conflictDetected) {
      val lastVersion = versions.flatten.reduceLeft((va, vb) => if (va.version > vb.version) {
        va
      } else {
        vb
      })
      /* TODO: write a basic conflict resolution code here! */
    }

    /* Write last value */
    partition.foreach(n => n.ref ! Write(key, value.get))
  }

  /*
  *
  * Partition management method
  *
  * */

  def join(joiningService: DistributedHashTableService) {

    /* Booking joined and joining joiningService */
    for {
      currentGetLocked <- actor ? WaitJoinToken()
      joiningGetLocked <- joiningService.actor ? WaitJoinToken()
      currentPartition <- getPartitionMembers()
      joiningPartition <- joiningService.getPartitionMembers()
    } yield {

      joiningPartition.map(s => s.ref ! MergeThisPartition(currentPartition))
      currentPartition.map(s => s.ref ! MergeThisPartition(joiningPartition))

      /* Releasing joined and joining joiningService */
      (joiningService.actor ? ReleaseJoinToken())
      (actor ! ReleaseJoinToken())
    }
  }

  def connect(service: DistributedHashTableService) {
    for {
      joiningNodeRef <- service.getNodeRef()
      joiningId <- service.getId()
      currentNodeRef <- getNodeRef()
      currentId <- getId()
      joinedsNodeRef <- findSuccessor(joiningId)
    } yield {
      (joinedsNodeRef.ref ! UpdateShortcuts(joiningNodeRef, joiningId))
      (joiningNodeRef.ref ! UpdateShortcuts(joinedsNodeRef, currentId))
    }
  }

  def getNodeRef(): Future[NodeRef] = for {
    nodeRef <- (actor ? GetNodeRef()).mapTo[NodeRef]
  } yield
    nodeRef

  def getPredecessor(): Future[NodeRef] = for {
    predecessor <- (actor ? GetPredecessor()).mapTo[NodeRef]
  } yield
    predecessor

  def getSuccessor(): Future[NodeRef] = for {
    successor <- (actor ? GetSuccessor()).mapTo[NodeRef]
  } yield
    successor

  def lookUp(id: Long, debug: Boolean = false): Future[Option[NodeRef]] = for {
    successor <- (actor ? LookUp(id, debug, this.id, Nil)).mapTo[Option[NodeRef]]
  } yield
    successor

  def findSuccessor(id: Long, debug: Boolean = false): Future[NodeRef] = for {
    successor <- (actor ? FindSuccessor(id, debug, this.id, Nil)).mapTo[NodeRef]
  } yield
    successor

  def getId(): Future[Long] = for {
    id <- (actor ? GetDhtId()).mapTo[Long]
  } yield
    id

  def debug() {
    actor ! Debug()
  }

  def getPartitionMembers(): Future[List[NodeRef]] =
    (actor ? GetPartitionMembers()).mapTo[List[NodeRef]]

}


class LocalDistributedHashTableServiceImpl(supervisor: NodeRef) extends DistributedHashTableServiceImpl(supervisor) {

  val _actor: ActorRef = {
    val ctx = TypedActor.context
    ctx.actorOf(Props(new DistributedHashTableActor(supervisor)), name = s"dht_actor_${supervisor.location.getId}")
  }

  def actor: ActorRef = _actor

}

class RemoteDistributedHashTableServiceImpl(supervisor: NodeRef, remote: ActorRef) extends DistributedHashTableServiceImpl(supervisor) {

  def actor = remote

}

