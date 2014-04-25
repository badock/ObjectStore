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

import org.discovery.AkkaArc.util.{NodeRef, INetworkLocation}
import org.discovery.AkkaArc.PeerActor
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy.Restart
import scala.concurrent.duration._
import org.discovery.AkkaArc.overlay.OverlayServiceFactory
import org.discovery.AkkaArc.notification.ChordServiceWithNotificationFactory
import org.discovery.dht.system.{DistributedHashTableFactory, DistributedHashTableService, DistributedHashTableMessage}

class DistributedHashTableSupervisor(location: INetworkLocation, overlayFactory: OverlayServiceFactory = ChordServiceWithNotificationFactory) extends PeerActor(location, overlayFactory) {

  var dhtService: DistributedHashTableService = DistributedHashTableFactory.createLocalService(system, selfNodeRef)

  override def receive = {

    case msg: DistributedHashTableMessage =>


    case msg =>
      super.receive(msg)
  }

  override def onConnection() {}

  override def onDisconnection() {}

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = 1 second) {
      case e: Exception =>
        e.printStackTrace()
        Restart
    }
}
