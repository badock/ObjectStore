package org.discovery.dht

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

import akka.testkit.{TestActorRef, TestKit}
import akka.actor.{Props, ActorSystem}
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.discovery.AkkaArc.util.{FakeNetworkLocation, NodeRef}
import org.discovery.dht.system.{DistributedHashTableActor, DistributedHashTableFactory}
import scala.concurrent.Await
import scala.concurrent.duration._

class ShortcutsTest extends TestKit(ActorSystem("testSystem2")) with WordSpec with MustMatchers {


  val mainDhtServiceNodeRef = NodeRef(FakeNetworkLocation(2), system.deadLetters)
  val props = Props(classOf[DistributedHashTableActor], mainDhtServiceNodeRef)
  val mainDhtServiceRef: TestActorRef[DistributedHashTableActor] = new TestActorRef[DistributedHashTableActor](system, props, system.deadLetters, "")

  val mainDhtService = DistributedHashTableFactory.connectRemoteService(system, mainDhtServiceNodeRef, mainDhtServiceRef)
  val dhtService3 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(3), system.deadLetters))
  val dhtService5 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(5), system.deadLetters))
  val dhtService7 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(7), system.deadLetters))
  val dhtService10 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(10), system.deadLetters))
  val dhtService60000 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(60000), system.deadLetters))
  val dhtService0 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(0), system.deadLetters))
  val dhtServiceMinus2 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(-2), system.deadLetters))

  dhtService3.connect(mainDhtService)
  dhtService5.connect(mainDhtService)
  dhtService7.connect(mainDhtService)
  dhtService10.connect(mainDhtService)
  dhtService60000.connect(mainDhtService)
  dhtService0.connect(mainDhtService)
  dhtServiceMinus2.connect(mainDhtService)

  Thread.sleep(3000)

  val mainActor = mainDhtServiceRef.underlyingActor

  "An actor [2] with a prefilled list of shortcuts [0,3,10,60000]" must {

    "must contain accurate shortcuts" in {

      List(3).foreach(i => assert(mainActor.shortcutsData.get(i).get.forall(s => s.node.location.getId == 3)))
      List(4).foreach(i => assert(mainActor.shortcutsData.get(i).get.forall(s => s.node.location.getId == 5)))
      List(6).foreach(i => assert(mainActor.shortcutsData.get(i).get.forall(s => s.node.location.getId == 7)))
      List(10).foreach(i => assert(mainActor.shortcutsData.get(i).get.forall(s => s.node.location.getId == 10)))
      List(18, 34, 66, 130, 258, 514, 1026, 2050, 4098, 8194, 16386, 32770).foreach(i => assert(mainActor.shortcutsData.get(i).get.forall(s => s.node.location.getId == 60000)))
      List(-9223372036854775807L).foreach(i => assert(mainActor.shortcutsData.get(i).get.forall(s => s.node.location.getId == -2)))

    }

    "must implement accurate isInInterval" in {

      // Case 1: a and b both postive and a < b
      List(10, 15, 30, 60).foreach(i => assert( mainActor.isInInterval(i, 10, 60)))
      List(9, 61, 120, -1).foreach(i => assert(!mainActor.isInInterval(i, 10, 60)))

      // Case 2: a and b both postive and a > b
      List(60, 3000, -10, 7, 8, 9, 10).foreach(i => assert( mainActor.isInInterval(i, 60, 10)))
      List(11, 15, 35, 45, 55, 57, 59).foreach(i => assert(!mainActor.isInInterval(i, 60, 10)))

      // Case 3: a < 0 and b > 0
      List(-2, -1, 5, 9, 10).foreach(i => assert( mainActor.isInInterval(i, -2, 10)))
      List(11, 600, -10, -3).foreach(i => assert(!mainActor.isInInterval(i, -2, 10)))

      // Case 4: a > 0 and b < 0
      List(10, 11, 600, -10, -2).foreach(i => assert( mainActor.isInInterval(i, 10, -2)))
      List(-1, 0, 1, 2, 3, 8, 9).foreach(i => assert(!mainActor.isInInterval(i, 10, -2)))

      // Case 5: a and b both negative and a > b
      List(-10, -9, -1, 0, 60, -60, -50, -20).foreach(i => assert( mainActor.isInInterval(i, -10, -20)))
      List(-19, -18, -15, -14, -13, -12, -11).foreach(i => assert(!mainActor.isInInterval(i, -10, -20)))

      // Case 6: all truc: if a and b both negative and a < b
      List(-20, -19, -15, -14, -11, -10).foreach(i => assert( mainActor.isInInterval(i, -20, -10)))
      List(-9, -1, 0, 50, 80, -21, -100).foreach(i => assert(!mainActor.isInInterval(i, -20, -10)))

    }

    "must implement accurate closestPrecedingNode" in {

      List(3).foreach(i => assert(mainActor.closestPrecedingNode(i).location.getId == 2))
      List(4,5).foreach(i => assert(mainActor.closestPrecedingNode(i).location.getId == 3))
      List(6,7).foreach(i => assert(mainActor.closestPrecedingNode(i).location.getId == 5))
      List(8,9,10).foreach(i => assert(mainActor.closestPrecedingNode(i).location.getId == 7))
      List(11,15,18, 34, 66, 130, 258, 514, 1026, 2050, 4098, 8194, 16386, 32770,60000).foreach(i => assert(mainActor.closestPrecedingNode(i).location.getId == 10))
      List(-9223372036854775807L,-2L).foreach(i =>  assert(mainActor.closestPrecedingNode(i).location.getId == 60000))
      List(-1,0,1,2).foreach(i =>  assert(mainActor.closestPrecedingNode(i).location.getId == -2))

    }

    "must implement accurate findSuccessor" in {

      val services = List(mainDhtService, dhtService3, dhtService5, dhtService7, dhtService10, dhtService60000, dhtService0, dhtServiceMinus2)

      services.foreach(s => assert({
        val successor =  Await.result(s.findSuccessor(1), 2 seconds)
        successor.location.getId == 2
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.findSuccessor(2), 2 seconds)
        successor.location.getId == 3
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.findSuccessor(4), 2 seconds)
        successor.location.getId == 5
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.findSuccessor(6), 2 seconds)
        successor.location.getId == 7
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.findSuccessor(9), 2 seconds)
        successor.location.getId == 10
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.findSuccessor(5900), 2 seconds)
        successor.location.getId == 60000
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.findSuccessor(-1), 2 seconds)
        successor.location.getId == 0
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.findSuccessor(-3), 2 seconds)
        successor.location.getId == -2
      }))

    }

    "must implement accurate lookUp" in {

      val services = List(mainDhtService, dhtService3, dhtService5, dhtService7, dhtService10, dhtService60000, dhtService0, dhtServiceMinus2)

      services.foreach(s => assert({
        val successor =  Await.result(s.lookUp(2), 2 seconds)
        successor.get.location.getId == 2
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.lookUp(3), 2 seconds)
        successor.get.location.getId == 3
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.lookUp(5), 2 seconds)
        successor.get.location.getId == 5
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.lookUp(7), 2 seconds)
        successor.get.location.getId == 7
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.lookUp(10), 2 seconds)
        successor.get.location.getId == 10
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.lookUp(60000), 2 seconds)
        successor.get.location.getId == 60000
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.lookUp(-2), 2 seconds)
        successor.get.location.getId == -2
      }))

      services.foreach(s => assert({
        val successor =  Await.result(s.lookUp(0), 2 seconds)
        successor.get.location.getId == 0
      }))

    }

  }
}
