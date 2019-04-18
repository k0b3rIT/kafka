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

import com.cloudera.kafka.consumer.metrics.ConsumerGroupMetricsHandler
import com.yammer.metrics.core.MetricName
import io.prometheus.client.{Gauge => PrometheusGauge}
import kafka.coordinator.group.GroupMetadataManager
import kafka.utils.Logging

import scala.collection.mutable

class PrometheusMetricsHandler(groupManagerProvider: Option[() => GroupMetadataManager]) extends ConsumerGroupMetricsHandler(groupManagerProvider) with Logging {

  import JmxMetricNames._
  import PrometheusMetricsHandler._

  private val allGauges = mutable.Map.empty[String, PrometheusGauge]

  // Increase Counter Metrics
  private val topicBytesInPerSecTotal = createGauge("topic_bytesinpersec_total", "Topic BytesInPerSec Count", Topic)
  private val topicBytesOutPerSecTotal = createGauge("topic_bytesoutpersec_total", "Topic BytesOutPerSec Count", Topic)
  private val topicMessagesInPerSecTotal = createGauge("topic_messagesinpersec_total", "Topic MessagesInPerSec Count", Topic)
  private val brokerBytesInPerSecTotal = createGauge("broker_bytesinpersec_total", "Broker BytesInPerSec Count")
  private val brokerBytesOutPerSecTotal = createGauge("broker_bytesoutpersec_total", "Broker BytesOutPerSec Count")
  private val brokerMessagesInPerSecTotal = createGauge("broker_messagesinpersec_total", "Broker MessagesInPerSec Count")
  private val brokerProducerMessagesInPerSecTotal = createGauge("broker_producer_messagesinpersec_total", "Producer MessagesInPerSec Count", ClientId, Topic, Partition)
  private val brokerUncleanLeaderElectionsPerSecTotal = createGauge("broker_uncleanleaderelectionspersec_total", "Broker UncleanLeaderElectionsPerSec Count")
  private val topicPartitionMessagesInPerSecTotal = createGauge("topic_partition_messagesinpersec_total", "Topic Partition MessagesInPerSec Count", Topic, Partition)
  private val topicPartitionBytesInPerSecTotal = createGauge("topic_partition_bytesinpersec_total", "Topic Partition BytesInPerSec Count", Topic, Partition)
  private val topicPartitionBytesOutPerSecTotal = createGauge("topic_partition_bytesoutpersec_total", "Topic Partition BytesOutPerSec Count", Topic, Partition)

  // Consumer Metrics
  private val topicPartitionLogEndOffset = createGauge("partition_log_endoffset", "Partition Log End Offset", Topic, Partition)
  private val groupCommittedOffset = createGauge("group_committed_offset", "Consumer Group Committed Offset", Group, Topic, Partition)

  // Other Kafka JMX Metrics (Non-Kafka Connect)
  private val partitionBytesInPerSec15MinRate = createGauge("topic_partition_bytesinpersec_fifteenminuterate", "Topic Partition BytesInPerSec 15min Rate", Topic, Partition)
  private val partitionBytesOutPerSec15MinRate = createGauge("topic_partition_bytesoutpersec_fifteenminuterate", "Topic Partition BytesOutPerSec 15min Rate", Topic, Partition)
  private val partitionMessagesInPerSec15MinRate = createGauge("topic_partition_messagesinpersec_fifteenminuterate", "Topic Partition MessagesInPerSec 15min Rate", Topic, Partition)

  private val topicBytesInPerSec15MinRate = createGauge("topic_bytesinpersec_fifteenminuterate", "Topic BytesInPerSec 15min Rate", Topic)
  private val topicBytesOutPerSec15MinRate = createGauge("topic_bytesoutpersec_fifteenminuterate", "Topic BytesOutPerSec 15min Rate", Topic)
  private val topicMessagesInPerSec15MinRate = createGauge("topic_messagesinpersec_fifteenminuterate", "Topic MessagesInPerSec 15min Rate", Topic)

  private val brokerBytesInPerSec15MinRate = createGauge("broker_bytesinpersec_fifteenminuterate", "Broker BytesInPerSec 15min Rate")
  private val brokerBytesOutPerSec15MinRate = createGauge("broker_bytesoutpersec_fifteenminuterate", "Broker BytesOutPerSec 15min Rate")
  private val brokerMessagesInPerSec15MinRate = createGauge("broker_messagesinpersec_fifteenminuterate", "Broker MessagesInPerSec 15min Rate")
  private val brokerTotalProduceRequestsPerSec15MinRate = createGauge("broker_totalproducerequestspersec_fifteenminuterate", "Broker TotalProduceRequestsPerSec 15min Rate")
  private val brokerTotalFetchRequestsPerSec15MinRate = createGauge("broker_totalfetchrequestspersec_fifteenminuterate", "Broker TotalFetchRequestsPerSec 15min Rate")
  private val brokerRequestHandlerAvgIdlePercent15MinRate = createGauge("broker_requesthandleravgidlepercent_fifteenminuterate", "Broker RequestHandlerAvgIdlePercent 15min Rate")
  private val brokerZooKeeperExpiresPerSec15MinRate = createGauge("broker_zookeeperexpirespersec_fifteenminuterate", "Broker ZooKeeperExpiresPerSec 15min Rate")
  private val brokerIsrShrinksPerSec15MinRate = createGauge("broker_isrshrinkspersec_fifteenminuterate", "Broker IsrShrinksPerSec 15min Rate")

  private val brokerPartitionCount = createGauge("broker_partitioncount", "Broker PartitionCount")
  private val brokerLeaderCount = createGauge("broker_leadercount", "Broker LeaderCount")
  private val brokerUnderReplicatedPartitions = createGauge("broker_underreplicatedpartitions", "Broker UnderReplicatedPartitions")
  private val brokerNetworkProcessorAvgIdlePercent = createGauge("broker_networkprocessoravgidlepercent", "Broker NetworkProcessorAvgIdlePercent")

  private val brokerLeaderElectionRateAndTimeMs15MinRate = createGauge("broker_leaderelectionrateandtimems_fifteenminuterate", "Broker LeaderElectionRateAndTimeMs 15min Rate")
  private val brokerActiveControllerCount = createGauge("broker_activecontrollercount", "Broker ActiveControllerCount")
  private val brokerOfflinePartitionsCount = createGauge("broker_offlinepartitionscount", "Broker OfflinePartitionsCount")

  private val partitionReplicasCount = createGauge("topic_partition_replicascount", "Topic Partition ReplicasCount", Topic, Partition)
  private val partitionInSyncReplicasCount = createGauge("topic_partition_insyncreplicascount", "Topic Partition InSyncReplicasCount", Topic, Partition)
  private val partitionUnderReplicated = createGauge("topic_partition_underreplicated", "Topic Partition UnderReplicated", Topic, Partition)

  private val brokerTotalTimeMsProduce99th = createGauge("broker_totaltimems_produce_99thpercentile", "Broker TotalTimeMs 99th Percentile")
  private val brokerTotalTimeMsFetchConsumer99th = createGauge("broker_totaltimems_fetchconsumer_99thpercentile", "Broker TotalTimeMs 99th Percentile")

  def getAllGauges: Map[String, PrometheusGauge] = Map(allGauges.toSeq: _*)

  def createGauge(name: String, help: String, labelNames: String*): PrometheusGauge = {
    val builder = PrometheusGauge.build.name(name)

    if (labelNames.nonEmpty) {
      builder.labelNames(labelNames: _*)
    }

    val gauge = builder.help(help).register()
    allGauges(name) = gauge

    gauge
  }

  def parseProperties(name: MetricName): collection.mutable.Map[String, String] = {
    val properties = name.getMBeanName.split(",")
    val propertyMap = collection.mutable.Map[String, String]()

    for (property <- properties) {
      val item = property.split("=")
      propertyMap.put(item(0), item(1))
    }

    propertyMap
  }

  def handleIncreasedCount(name: MetricName, increasedCount: Double): Unit = {
    val propertyMap = parseProperties(name)

    val topic = propertyMap.getOrElse(Topic, "")
    val partition = propertyMap.getOrElse(Partition, "")
    val clientId = propertyMap.getOrElse(ClientId, "")

     name.getName match {
       case BytesInPerSec =>
         if (topic.nonEmpty && partition.nonEmpty) {
           topicPartitionBytesInPerSecTotal.labels(topic, partition).set(increasedCount)
         } else if (topic.nonEmpty && partition.isEmpty) {
           topicBytesInPerSecTotal.labels(topic).set(increasedCount)
         } else if (topic.isEmpty && partition.isEmpty) {
           brokerBytesInPerSecTotal.set(increasedCount)
         }
       case BytesOutPerSec =>
         if (topic.nonEmpty && partition.nonEmpty) {
           topicPartitionBytesOutPerSecTotal.labels(topic, partition).set(increasedCount)
         } else if (topic.nonEmpty && partition.isEmpty) {
           topicBytesOutPerSecTotal.labels(topic).set(increasedCount)
         } else if (topic.isEmpty && partition.isEmpty) {
           brokerBytesOutPerSecTotal.set(increasedCount)
         }
       case MessagesInPerSec =>
         if (clientId.nonEmpty && topic.nonEmpty && partition.nonEmpty) {
           brokerProducerMessagesInPerSecTotal.labels(clientId, topic, partition).set(increasedCount)
         } else if (topic.nonEmpty && partition.nonEmpty) {
           topicPartitionMessagesInPerSecTotal.labels(topic, partition).set(increasedCount)
         } else if (topic.nonEmpty && partition.isEmpty) {
           topicMessagesInPerSecTotal.labels(topic).set(increasedCount)
         } else if (topic.isEmpty && partition.isEmpty) {
           brokerMessagesInPerSecTotal.set(increasedCount)
         }
       case UncleanLeaderElectionsPerSec =>
         if (topic.isEmpty && partition.isEmpty) {
           brokerUncleanLeaderElectionsPerSecTotal.set(increasedCount)
         }
       case _ =>
         trace(s"Skipping MBean ${name.getMBeanName} from Prometheus metrics handling")
     }
  }

  override def handleGroupCommittedOffset(group: String,
                                                      topic: String,
                                                      partition: String,
                                                      offset: Long): Unit = {
    groupCommittedOffset.labels(group, topic, partition).set(offset.toDouble)
  }

  def handleKafkaMetric(name: MetricName, value: Double): Unit = {
    val propertyMap = parseProperties(name)

    val topic = propertyMap.getOrElse(Topic, "")
    val partition = propertyMap.getOrElse(Partition, "")

    name.getName match {
      case BytesInPerSec =>
        if (topic.nonEmpty && partition.nonEmpty) {
          partitionBytesInPerSec15MinRate.labels(topic, partition).set(value)
        } else if (topic.nonEmpty && partition.isEmpty) {
          topicBytesInPerSec15MinRate.labels(topic).set(value)
        } else if (topic.isEmpty && partition.isEmpty) {
          brokerBytesInPerSec15MinRate.set(value)
        }
      case BytesOutPerSec =>
        if (topic.nonEmpty && partition.nonEmpty) {
          partitionBytesOutPerSec15MinRate.labels(topic, partition).set(value)
        } else if (topic.nonEmpty && partition.isEmpty) {
          topicBytesOutPerSec15MinRate.labels(topic).set(value)
        } else if (topic.isEmpty && partition.isEmpty) {
          brokerBytesOutPerSec15MinRate.set(value)
        }
      case MessagesInPerSec =>
        if (topic.nonEmpty && partition.nonEmpty) {
          partitionMessagesInPerSec15MinRate.labels(topic, partition).set(value)
        } else if (topic.nonEmpty && partition.isEmpty) {
          topicMessagesInPerSec15MinRate.labels(topic).set(value)
        } else if (topic.isEmpty && partition.isEmpty) {
          brokerMessagesInPerSec15MinRate.set(value)
        }
      case TotalProduceRequestsPerSec =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerTotalProduceRequestsPerSec15MinRate.set(value)
        }
      case TotalFetchRequestsPerSec =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerTotalFetchRequestsPerSec15MinRate.set(value)
        }
      case RequestHandlerAvgIdlePercent =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerRequestHandlerAvgIdlePercent15MinRate.set(value)
        }
      case ZooKeeperExpiresPerSec =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerZooKeeperExpiresPerSec15MinRate.set(value)
        }
      case IsrShrinksPerSec =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerIsrShrinksPerSec15MinRate.set(value)
        }
      case PartitionCount =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerPartitionCount.set(value)
        }
      case LeaderCount =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerLeaderCount.set(value)
        }
      case UnderReplicatedPartitions =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerUnderReplicatedPartitions.set(value)
        }
      case NetworkProcessorAvgIdlePercent =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerNetworkProcessorAvgIdlePercent.set(value)
        }
      case LeaderElectionRateAndTimeMs =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerLeaderElectionRateAndTimeMs15MinRate.set(value)
        }
      case ActiveControllerCount =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerActiveControllerCount.set(value)
        }
      case OfflinePartitionsCount =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerOfflinePartitionsCount.set(value)
        }
      case ReplicasCount =>
        if (topic.nonEmpty && partition.nonEmpty) {
          partitionReplicasCount.labels(topic, partition).set(value)
        }
      case InSyncReplicasCount =>
        if (topic.nonEmpty && partition.nonEmpty) {
          partitionInSyncReplicasCount.labels(topic, partition).set(value)
        }
      case UnderReplicated =>
        if (topic.nonEmpty && partition.nonEmpty) {
          partitionUnderReplicated.labels(topic, partition).set(value)
        }
      case TotalTimeMs if name.getScope == RequestProduce =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerTotalTimeMsProduce99th.set(value)
        }
      case TotalTimeMs if name.getScope == RequestFetchConsumer =>
        if (topic.isEmpty && partition.isEmpty) {
          brokerTotalTimeMsFetchConsumer99th.set(value)
        }
      case LogEndOffset =>
        if (topic.nonEmpty && partition.nonEmpty) {
          topicPartitionLogEndOffset.labels(topic, partition).set(value)
        }
      case _ =>
        debug(s"Skipping MBean ${name.getMBeanName} from Prometheus metrics handling")
    }
  }

}

object PrometheusMetricsHandler {
  private val Topic = "topic"
  private val ClientId = "clientId"
  private val Partition = "partition"
  private val Group = "group"
}
