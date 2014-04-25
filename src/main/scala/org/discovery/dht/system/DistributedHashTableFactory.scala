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

import akka.actor._
import org.discovery.AkkaArc.util.NodeRef

object DistributedHashTableFactory {

  def createLocalService(system: ActorSystem, supervisor: NodeRef): DistributedHashTableService = {

    TypedActor(system).typedActorOf(TypedProps(classOf[DistributedHashTableService],
      new LocalDistributedHashTableServiceImpl(supervisor)),
      name = s"dht_service_${supervisor.location.getId}")
  }

  def connectRemoteService(system: ActorSystem, supervisor: NodeRef, remoteActor: ActorRef): DistributedHashTableService = {

    TypedActor(system).typedActorOf(TypedProps(classOf[DistributedHashTableService],
      new RemoteDistributedHashTableServiceImpl(supervisor, remoteActor)))
  }

}
