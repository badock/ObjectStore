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
import scala.collection.mutable

trait DistributedHashTableMessage

object DistributedHashTableProtocol {

  case class Get(key: String) extends DistributedHashTableMessage
  case class Write(key: String, value: Version) extends DistributedHashTableMessage
  case class Remove(key: String) extends DistributedHashTableMessage
  case class FusionKeys(map: mutable.HashMap[String, Version]) extends DistributedHashTableMessage

  case class GetDhtId() extends DistributedHashTableMessage
  case class GetNodeRef() extends DistributedHashTableMessage
  case class MergeThisPartition(partition: List[NodeRef]) extends DistributedHashTableMessage
  case class GetPartitionMembers() extends DistributedHashTableMessage
  case class UpdateShortcuts(remoteNode: NodeRef, remoteId: Long) extends DistributedHashTableMessage

  case class WaitJoinToken() extends DistributedHashTableMessage
  case class ReleaseJoinToken() extends DistributedHashTableMessage

  case class GetPredecessor() extends DistributedHashTableMessage
  case class GetSuccessor() extends DistributedHashTableMessage
  case class LookUp(id: Long, debug:Boolean, origin: Long, already: List[Long]) extends DistributedHashTableMessage
  case class FindSuccessor(id: Long, debug:Boolean, origin: Long, already: List[Long]) extends DistributedHashTableMessage

  case class Notify(remote: NodeRef) extends DistributedHashTableMessage

  case class Tick() extends DistributedHashTableMessage
  case class Debug() extends DistributedHashTableMessage
}
