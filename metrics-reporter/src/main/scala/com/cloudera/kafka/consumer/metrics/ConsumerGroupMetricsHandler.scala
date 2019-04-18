/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package com.cloudera.kafka.consumer.metrics

import com.cloudera.kafka.prometheus.metrics.reporting.GroupMetaData
import kafka.coordinator.group.GroupMetadataManager
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.ConsumerGroupState

import java.nio.ByteBuffer

abstract class ConsumerGroupMetricsHandler(groupManagerProvider: Option[() => GroupMetadataManager]) extends Logging {

  def handleGroupCommittedOffset(group: String, topic: String, partition: String, offset: Long): Unit

  def processConsumerGroupData(): Unit = {
    getValidConsumerGroups.foreach { groupMetaData =>

      if (groupMetaData.summary.members.nonEmpty) {
        groupMetaData.summary.members.foreach { member =>
          if (member.assignment.length > 0) {
            val topicPartitions = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.assignment)).partitions()
            topicPartitions.forEach { topicPartition =>
              if (groupMetaData.allOffsets.contains(topicPartition)) {
                handleGroupCommittedOffset(
                  groupMetaData.groupId,
                  topicPartition.topic(),
                  topicPartition.partition().toString,
                  groupMetaData.allOffsets(topicPartition).offset)
              }
            }
          }
        }
      }
    }
  }

  def getValidConsumerGroups: Iterable[GroupMetaData] = {
    if (groupManagerProvider.isEmpty)
      return Iterable.empty
    val currentGroups = groupManagerProvider.get().currentGroups
      .filter(groupMetaData => groupMetaData.groupId != null && groupMetaData.groupId.nonEmpty
        && groupMetaData.summary.protocolType.contains(ConsumerProtocol.PROTOCOL_TYPE)
        && groupMetaData.summary.state != ConsumerGroupState.DEAD.toString)

    currentGroups.map(groupMetaData => GroupMetaData(groupMetaData.groupId, groupMetaData.summary, groupMetaData.allOffsets))
  }

}