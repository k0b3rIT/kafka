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

import kafka.utils.Logging
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.message.OffsetFetchRequestData.{OffsetFetchRequestGroup, OffsetFetchRequestTopics}
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup
import org.apache.kafka.common.message.{DescribeGroupsResponseData, ListGroupsRequestData}
import org.apache.kafka.coordinator.group.GroupCoordinator

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

abstract class ConsumerGroupMetricsHandler(groupCoordinator: Option[() => GroupCoordinator]) extends Logging {

  def handleGroupCommittedOffset(group: String, topic: String, partition: String, offset: Long): Unit

  def processConsumerGroupData(): Unit = {
    getValidConsumerGroups.foreach { groupMetaData =>
      if (! groupMetaData.topics().isEmpty) {
        groupMetaData.topics().forEach { topic =>
          if (! topic.partitions().isEmpty) {
            topic.partitions().forEach { partition =>
              handleGroupCommittedOffset(
                groupMetaData.groupId,
                topic.name(),
                partition.partitionIndex().toString,
                partition.committedOffset()
              )
            }
          }
        }
      }
    }
  }

  def getValidConsumerGroups: Iterable[OffsetFetchResponseGroup] = {
    if (groupCoordinator.isEmpty)
      return Iterable.empty
    val currentGroups = groupCoordinator.get().listGroups(null, new ListGroupsRequestData).get().groups().asScala
      .filter(groupMetaData => groupMetaData.groupId != null && groupMetaData.groupId.nonEmpty
        && groupMetaData.protocolType().contains(ConsumerProtocol.PROTOCOL_TYPE)
        && groupMetaData.groupState() != ConsumerGroupState.DEAD.toString)

    val describedGroupList = groupCoordinator.get()
      .describeGroups(null, currentGroups.map(groupMetaData => groupMetaData.groupId()).toList.asJava)
      .get().asScala

    getOffsetsForConsumerGroups(describedGroupList)
  }

  private def getOffsetsForConsumerGroups(describedGroupList: Iterable[DescribeGroupsResponseData.DescribedGroup]): Iterable[OffsetFetchResponseGroup] = {
    describedGroupList.flatMap { describedGroup =>
      describedGroup.members().asScala.map { member =>
        groupCoordinator
          .get()
          .fetchOffsets(null, createOffsetFetchRequestGroup(describedGroup, member), true)
          .get()
      }
    }
  }

  private def createOffsetFetchRequestGroup(describedGroup: DescribeGroupsResponseData.DescribedGroup, member: DescribeGroupsResponseData.DescribedGroupMember): OffsetFetchRequestGroup = {
    val assigment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.memberAssignment()))
    val topicPartitions = assigment.partitions().asScala.groupBy(tp=>tp.topic())

    new OffsetFetchRequestGroup()
      .setGroupId(describedGroup.groupId())
      .setMemberId(member.memberId())
      .setTopics(
        topicPartitions.map(
          es => {
            val topic = es._1
            val partitionsIndexes = es._2.map(tp => Integer.valueOf(tp.partition())).toList
            new OffsetFetchRequestTopics()
              .setName(topic)
              .setPartitionIndexes(partitionsIndexes.asJava)
          })
          .toList.asJava)
  }
}