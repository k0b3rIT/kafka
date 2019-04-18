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

package com.cloudera.kafka.prometheus.metrics.reporting

object JmxMetricNames {
  val KafkaServerGroup = "kafka.server"
  val KafkaControllerGroup = "kafka.controller"
  val KafkaNetworkGroup = "kafka.network"
  val KafkaClusterGroup = "kafka.cluster"
  val KafkaLog = "kafka.log"
  val Log = "Log"
  val SocketServer = "SocketServer"
  val KafkaController = "KafkaController"
  val RequestMetrics = "RequestMetrics"
  val PartitionType = "Partition"
  val BrokerTopicMetrics = "BrokerTopicMetrics"
  val BrokerClientMetrics = "BrokerClientMetrics"
  val ControllerStats = "ControllerStats"
  val KafkaRequestHandlerPool = "KafkaRequestHandlerPool"
  val SessionExpireListener = "SessionExpireListener"
  val ReplicaManager = "ReplicaManager"
  val BytesInPerSec = "BytesInPerSec"
  val BytesOutPerSec = "BytesOutPerSec"
  val MessagesInPerSec = "MessagesInPerSec"
  val UncleanLeaderElectionsPerSec = "UncleanLeaderElectionsPerSec"
  val TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec"
  val TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec"
  val ZooKeeperExpiresPerSec = "ZooKeeperExpiresPerSec"
  val IsrShrinksPerSec = "IsrShrinksPerSec"
  val RequestHandlerAvgIdlePercent = "RequestHandlerAvgIdlePercent"
  val PartitionCount = "PartitionCount"
  val LeaderCount = "LeaderCount"
  val LogEndOffset = "LogEndOffset"
  val UnderReplicatedPartitions = "UnderReplicatedPartitions"
  val NetworkProcessorAvgIdlePercent = "NetworkProcessorAvgIdlePercent"
  val TotalTimeMs = "TotalTimeMs"
  val LeaderElectionRateAndTimeMs = "LeaderElectionRateAndTimeMs"
  val ActiveControllerCount = "ActiveControllerCount"
  val OfflinePartitionsCount = "OfflinePartitionsCount"
  val ReplicasCount = "ReplicasCount"
  val InSyncReplicasCount = "InSyncReplicasCount"
  val UnderReplicated = "UnderReplicated"
  val RequestFetchConsumer = "request.FetchConsumer"
  val RequestProduce = "request.Produce"
}
