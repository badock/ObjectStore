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
import akka.actor.ActorSystem
import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.discovery.dht.system.{Version, DistributedHashTableService, DistributedHashTableFactory}
import org.discovery.AkkaArc.util.{NodeRef, FakeNetworkLocation}
import scala.concurrent.Await
import scala.concurrent.duration._

class DistributedHashTableTest extends TestKit(ActorSystem("testSystem")) with WordSpec with MustMatchers {

  "A single instance of DistributedHashTable" must {

    "implement a working \"localWrite\" method" in {

      val a=1
      val b=2


      val dhtService = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(0), system.deadLetters))
      dhtService.localWrite("a", a)
      dhtService.localWrite("b", b)
    }

    "implement a working \"localGet\" method" in {

      val a=1
      val b=2


      val dhtService = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(1), system.deadLetters))
      dhtService.localWrite("a", a)
      dhtService.localWrite("b", b)

      /* test get method */
      val getA = Await.result(dhtService.localGet("a"), 2 seconds)
      val getB = Await.result(dhtService.localGet("b"), 2 seconds)

      assert(getA match {
        case Some(version) => version.value match {
          case 1 => true
          case _ => false
        }
        case _ => false
      })

      assert(getB match {
        case Some(version) => version.value match {
          case 2 => true
          case _ => false
        }
        case _ => false
      })
    }
  }



  "Several instances DistributedHashTable of the same partition" must {

    "implement a working \"localWrite\" method" in {

      val a=1
      val b=2
      val c=3

      val dhtService1 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(11), system.deadLetters))
      val dhtService2 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(21), system.deadLetters))
      val dhtService3 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(31), system.deadLetters))

      dhtService1.localWrite("a", a)
      dhtService2.localWrite("b", b)
      dhtService3.localWrite("c", c)
    }

    "implement a working \"localGet\" method" in {

      val a=1
      val b=2
      val c=3

      val dhtService1 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(12), system.deadLetters))
      val dhtService2 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(22), system.deadLetters))
      val dhtService3 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(32), system.deadLetters))

      dhtService1.join(dhtService2)
      dhtService2.join(dhtService3)


      Thread.sleep(500)

      dhtService1.localWrite("a", a)
      dhtService2.localWrite("b", b)
      dhtService3.localWrite("c", c)

      Thread.sleep(500)

      /* test get method */
      val getA = Await.result(dhtService2.localGet("a"), 2 seconds)
      val getB = Await.result(dhtService3.localGet("b"), 2 seconds)
      val getC = Await.result(dhtService1.localGet("c"), 2 seconds)

      assert(getA match {
        case Some(version) => version.value match {
          case 1 => true
          case _ => false
        }
        case _ => false
      })

      assert(getB match {
        case Some(version) => version.value match {
          case 2 => true
          case _ => false
        }
        case _ => false
      })

      assert(getC match {
        case Some(version) => version.value match {
          case 3 => true
          case _ => false
        }
        case _ => false
      })
    }


    "implement a working \"localGet\" method (complex version)" in {

      val a=1
      val b=2
      val c=3

      val dhtService1 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(13), system.deadLetters))
      dhtService1.localWrite("a", a)
      val dhtService2 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(23), system.deadLetters))
      dhtService2.localWrite("b", b)
      val dhtService3 = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(33), system.deadLetters))
      dhtService3.localWrite("c", c)

      dhtService1.join(dhtService2)
      dhtService2.join(dhtService3)

      Thread.sleep(500)

      /* test get method */
      val getA = Await.result(dhtService2.localGet("a"), 2 seconds)
      val getB = Await.result(dhtService3.localGet("b"), 2 seconds)
      val getC = Await.result(dhtService1.localGet("c"), 2 seconds)

      assert(getA match {
        case Some(version) => version.value match {
          case 1 => true
          case _ => false
        }
        case _ => false
      })

      assert(getB match {
        case Some(version) => version.value match {
          case 2 => true
          case _ => false
        }
        case _ => false
      })

      assert(getC match {
        case Some(version) => version.value match {
          case 3 => true
          case _ => false
        }
        case _ => false
      })
    }

    "implement a working \"get\" and \"put\" method (complex version)" in {

      val system = ActorSystem(s"LocalTestSystem")



      val mainDhtService = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation( Util.md5("0")), system.deadLetters))
      var services: List[DistributedHashTableService] = List(mainDhtService)

      for (i <- 1 to 20) {
        val dhtService = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(Util.md5(s"$i")), system.deadLetters))
        dhtService.connect(mainDhtService)
        services = services ::: List(dhtService)
      }


      Thread.sleep(3000)

      for (i <- 0 to 20) {
        services(i).put(s"$i", i)
      }

      Thread.sleep(3000)

      services.foreach(s => {
        for (i <- 0 to 20) {
          val version = Await.result(s.get(s"$i"), 2 seconds)
          assert(version match {
            case Some(Version(_, j: Int, _)) if(i == j) =>
              true
            case _ =>
              false
          })
        }
      })

    }

  }

}
