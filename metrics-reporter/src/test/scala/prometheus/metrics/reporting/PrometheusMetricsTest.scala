/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package prometheus.metrics.reporting

import com.cloudera.kafka.prometheus.metrics.reporting.JmxMetricNames._
import com.cloudera.kafka.prometheus.metrics.reporting.{PrometheusMetricsHandler, PrometheusMetricsServlet}
import com.yammer.metrics.core.MetricsRegistry
import io.prometheus.client.CollectorRegistry
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.OffsetFetchResponseData
import org.apache.kafka.common.message.OffsetFetchResponseData.{OffsetFetchResponsePartitions, OffsetFetchResponseTopics}
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito

import java.util.Collections
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._


class PrometheusMetricsTest {

  val prometheusMetricsServlet: PrometheusMetricsServlet = Mockito.spy(new PrometheusMetricsServlet(new PrometheusMetricsHandler(null: Option[() => GroupCoordinator])))
  val prometheusMetricsHandler: PrometheusMetricsHandler = Mockito.spy(prometheusMetricsServlet.getPrometheusMetricsHandler)
  val metricsRegistry: MetricsRegistry = KafkaYammerMetrics.defaultRegistry()

  @BeforeEach
  def setup(): Unit = {
    KafkaYammerMetrics.defaultRegistry().addListener(prometheusMetricsServlet)
    Mockito.reset(prometheusMetricsServlet)
    prometheusMetricsServlet.clearPreviousMetrics()
  }

  @AfterEach
  def cleanup(): Unit = {
    KafkaYammerMetrics.defaultRegistry().removeListener(prometheusMetricsServlet)
    KafkaYammerMetrics.defaultRegistry().allMetrics().forEach({
      case (metricName, _) => KafkaYammerMetrics.defaultRegistry().removeMetric(metricName)
    })
  }

  @Test
  def testPrometheusMetricsServlet(): Unit = {
    val topic = "testTopic"
    val streamTopic = "streamTopic"
    val partition = "1"
    val partition2 = "2"
    val clientId1 = "testClientId1"
    val groupId = "testGroupId"
    val tp1 = new TopicPartition(topic, partition.toInt)
    val tp2 = new TopicPartition(topic, partition2.toInt)
    val tp3 = new TopicPartition(streamTopic, partition.toInt)
    val assignment1 = ConsumerProtocol.serializeAssignment(new Assignment(List(tp1).asJava))
    val assignment2 = ConsumerProtocol.serializeAssignment(new Assignment(List(tp2).asJava))
    val assignment3 = ConsumerProtocol.serializeAssignment(new Assignment(List(tp3).asJava))
    val assignment1ByteArray = new Array[Byte](assignment1.remaining())
    val assignment2ByteArray = new Array[Byte](assignment2.remaining())
    val assignment3ByteArray = new Array[Byte](assignment3.remaining())
    assignment1.get(assignment1ByteArray)
    assignment2.get(assignment2ByteArray)
    assignment3.get(assignment3ByteArray)

    val logEndOffset = 521L
    val committedOffset1 = 492
    val committedOffset2 = 599
    val partitionOffset1 = new OffsetFetchResponsePartitions().setPartitionIndex(tp1.partition()).setCommittedOffset(committedOffset1)
    val partitionOffset2 = new OffsetFetchResponsePartitions().setPartitionIndex(tp2.partition()).setCommittedOffset(committedOffset2)
    val partitionOffset3 = new OffsetFetchResponsePartitions().setPartitionIndex(tp3.partition())

    val topic1 = new OffsetFetchResponseTopics().setName(topic).setPartitions(List(partitionOffset1, partitionOffset2).asJava)
    val topic2 = new OffsetFetchResponseTopics().setName(streamTopic).setPartitions(List(partitionOffset3).asJava)
    val groupMetaData = new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId(groupId).setTopics(List(topic1, topic2).asJava)

    setupConsumerRelatedMock(topic, partition.toInt, List(groupMetaData))

    // Broker level metrics
    val brokerBytesInPerSec = 508
    val brokerBytesOutPerSec = 411
    val brokerUncleanLeaderPerSec = 12
    val brokerMessagesInPerSec = 602
    val partitionCount = 33
    val leaderCount = 11
    val underReplicatedPartitions = 3
    val networkProcessorAvgIdlePercent = 51.0
    val activeControllerCount = 1
    val offlinePartitionsCount = 6

    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, BytesInPerSec, Collections.emptyMap()), "bytes",  TimeUnit.SECONDS)
      .mark(brokerBytesInPerSec)
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, BytesOutPerSec, Collections.emptyMap()), "bytes",  TimeUnit.SECONDS)
      .mark(brokerBytesOutPerSec)
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaControllerGroup, ControllerStats, UncleanLeaderElectionsPerSec, Collections.emptyMap()), "elections",  TimeUnit.SECONDS)
      .mark(brokerUncleanLeaderPerSec)
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, MessagesInPerSec, Collections.emptyMap()), "elections",  TimeUnit.SECONDS)
      .mark(brokerMessagesInPerSec)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, ReplicaManager, PartitionCount, Collections.emptyMap()), () => partitionCount)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, ReplicaManager, LeaderCount, Collections.emptyMap()), () => leaderCount)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, ReplicaManager, UnderReplicatedPartitions, Collections.emptyMap()), () => underReplicatedPartitions)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaNetworkGroup, SocketServer, NetworkProcessorAvgIdlePercent, Collections.emptyMap()), () => networkProcessorAvgIdlePercent)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaControllerGroup, KafkaController, ActiveControllerCount, Collections.emptyMap()), () => activeControllerCount)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaControllerGroup, KafkaController, OfflinePartitionsCount, Collections.emptyMap()), () => offlinePartitionsCount)

    val produceTotalTimeMs = 80273
    val requestProduceTag = Map("request" -> "Produce").asJava
    metricsRegistry.newHistogram(KafkaMetricsGroup.explicitMetricName(KafkaNetworkGroup, RequestMetrics, TotalTimeMs, requestProduceTag), true)
      .update(produceTotalTimeMs)

    val fetchConsumerTotalTimeMs = 5039
    val requestFetchConsumerTag = Map("request" -> "FetchConsumer").asJava
    metricsRegistry.newHistogram(KafkaMetricsGroup.explicitMetricName(KafkaNetworkGroup, RequestMetrics, TotalTimeMs, requestFetchConsumerTag), true)
      .update(fetchConsumerTotalTimeMs)

    // Topic level metrics
    val topicMessagesInPerSec = 225
    val topicBytesInPerSec = 351
    val topicBytesOutPerSec = 672

    val topicLevelTag = Map("topic" -> topic).asJava
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, MessagesInPerSec, topicLevelTag), "messages",  TimeUnit.SECONDS)
      .mark(topicMessagesInPerSec)
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, BytesInPerSec, topicLevelTag), "bytes",  TimeUnit.SECONDS)
      .mark(topicBytesInPerSec)
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, BytesOutPerSec, topicLevelTag), "bytes",  TimeUnit.SECONDS)
      .mark(topicBytesOutPerSec)

    // Partition level metrics
    val partitionMessagesInPerSec = 211
    val partitionBytesInPerSec = 98
    val partitionBytesOutPerSec = 27
    val replicasCount = 22
    val inSyncReplicasCount = 39
    val underReplicated = 4

    val partitionLevelTags = Map("topic" -> topic, "partition" -> partition).asJava
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, MessagesInPerSec, partitionLevelTags), "messages",  TimeUnit.SECONDS)
      .mark(partitionMessagesInPerSec)
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, BytesInPerSec, partitionLevelTags), "bytes",  TimeUnit.SECONDS)
      .mark(partitionBytesInPerSec)
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, BytesOutPerSec, partitionLevelTags), "bytes",  TimeUnit.SECONDS)
      .mark(partitionBytesOutPerSec)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaClusterGroup, PartitionType, ReplicasCount, partitionLevelTags), () => replicasCount)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaClusterGroup, PartitionType, InSyncReplicasCount, partitionLevelTags), () => inSyncReplicasCount)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaClusterGroup, PartitionType, UnderReplicated, partitionLevelTags), () => underReplicated)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaLog, Log, LogEndOffset, partitionLevelTags), () => logEndOffset)

    // Not allowed metrics. These should be ignored by PrometheusMetricsServlet
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaLog, Log, "nonExistentMetric", partitionLevelTags), () => 1)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaClusterGroup, PartitionType, "nonExistentMetric", partitionLevelTags), () => 1)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaNetworkGroup, SocketServer, "nonExistentMetric", Collections.emptyMap()), () => 1)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaControllerGroup, ControllerStats, "nonExistentMetric", Collections.emptyMap()), () => 1)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerTopicMetrics, "nonExistentMetric", Collections.emptyMap()), () => 1)
    metricsRegistry.newGauge(KafkaMetricsGroup.explicitMetricName("nonExistentMetric", BrokerTopicMetrics, "nonExistentMetric", Collections.emptyMap()), () => 1)

    // Client Id level metrics
    val clientMessagesInPerSec = 89

    val clientLevelTags = Map("clientId"-> clientId1, "topic" -> topic, "partition" -> partition).asJava
    metricsRegistry.newMeter(KafkaMetricsGroup.explicitMetricName(KafkaServerGroup, BrokerClientMetrics, MessagesInPerSec, clientLevelTags), "messages",  TimeUnit.SECONDS)
      .mark(clientMessagesInPerSec)

    prometheusMetricsServlet.updatePrometheusMetrics(false)
    val gauges = prometheusMetricsServlet.getPrometheusGauges

    // Verify all 40 metrics are registered by Prometheus
    verifyAllMetricsAreRegistered()

    def checkAllMetrics(): Unit = {
      // Check Consumer Metrics
      assertEquals(gauges("partition_log_endoffset").labels(topic, partition).get().toLong, logEndOffset)
      assertEquals(gauges("group_committed_offset").labels(groupId, topic, partition).get().toLong, committedOffset1)
      assertEquals(gauges("group_committed_offset").labels(groupId, topic, partition2).get().toLong, committedOffset2)

      // Check Kafka Increased Count Metrics
      assertEquals(gauges("broker_bytesinpersec_total").get().toLong, brokerBytesInPerSec)
      assertEquals(gauges("broker_bytesoutpersec_total").get().toLong, brokerBytesOutPerSec)
      assertEquals(gauges("broker_messagesinpersec_total").get().toLong, brokerMessagesInPerSec)
      assertEquals(gauges("broker_uncleanleaderelectionspersec_total").get().toLong, brokerUncleanLeaderPerSec)
      assertEquals(gauges("topic_bytesinpersec_total").labels(topic).get().toLong, topicBytesInPerSec)
      assertEquals(gauges("topic_bytesoutpersec_total").labels(topic).get().toLong, topicBytesOutPerSec)
      assertEquals(gauges("topic_messagesinpersec_total").labels(topic).get().toLong, topicMessagesInPerSec)
      assertEquals(gauges("topic_partition_messagesinpersec_total").labels(topic, partition).get().toLong, partitionMessagesInPerSec)
      assertEquals(gauges("topic_partition_bytesinpersec_total").labels(topic, partition).get().toLong, partitionBytesInPerSec)
      assertEquals(gauges("topic_partition_bytesoutpersec_total").labels(topic, partition).get().toLong, partitionBytesOutPerSec)
      assertEquals(gauges("broker_producer_messagesinpersec_total").labels(clientId1, topic, partition).get().toLong, clientMessagesInPerSec)

      // Check Other Kafka Metrics
      assertEquals(gauges("broker_partitioncount").get().toInt, partitionCount)
      assertEquals(gauges("broker_leadercount").get().toInt, leaderCount)
      assertEquals(gauges("broker_underreplicatedpartitions").get().toInt, underReplicatedPartitions)
      assertEquals(gauges("broker_networkprocessoravgidlepercent").get().toInt, networkProcessorAvgIdlePercent.toInt)
      assertEquals(gauges("broker_activecontrollercount").get().toInt, activeControllerCount)
      assertEquals(gauges("broker_offlinepartitionscount").get().toInt, offlinePartitionsCount)
      assertEquals(gauges("topic_partition_replicascount").labels(topic, partition).get().toInt, replicasCount)
      assertEquals(gauges("topic_partition_insyncreplicascount").labels(topic, partition).get().toInt, inSyncReplicasCount)
      assertEquals(gauges("topic_partition_underreplicated").labels(topic, partition).get().toInt, underReplicated)
      assertEquals(gauges("broker_totaltimems_produce_99thpercentile").get().toInt, produceTotalTimeMs)
      assertEquals(gauges("broker_totaltimems_fetchconsumer_99thpercentile").get().toInt, fetchConsumerTotalTimeMs)
    }

    // Check Consumer Metrics, Kafka Increased Count Metrics, Other Kafka Metrics are marked correctly
    checkAllMetrics()

    // Updating the Prometheus metrics should provide the same marked result as before since the previous call didn't update the cache (false flag)
    prometheusMetricsServlet.updatePrometheusMetrics(true)
    checkAllMetrics()

    // Kafka Increased Count Metrics aren't marked between 2 calls, therefore the increase count should be 0 for all
    prometheusMetricsServlet.updatePrometheusMetrics(true)
    assertEquals(gauges("broker_bytesinpersec_total").get().toLong, 0)
    assertEquals(gauges("broker_bytesoutpersec_total").get().toLong, 0)
    assertEquals(gauges("broker_messagesinpersec_total").get().toLong, 0)
    assertEquals(gauges("broker_uncleanleaderelectionspersec_total").get().toLong, 0)
    assertEquals(gauges("topic_bytesinpersec_total").labels(topic).get().toLong, 0)
    assertEquals(gauges("topic_bytesoutpersec_total").labels(topic).get().toLong, 0)
    assertEquals(gauges("topic_messagesinpersec_total").labels(topic).get().toLong, 0)
    assertEquals(gauges("topic_partition_messagesinpersec_total").labels(topic, partition).get().toLong, 0)
    assertEquals(gauges("topic_partition_bytesinpersec_total").labels(topic, partition).get().toLong, 0)
    assertEquals(gauges("topic_partition_bytesoutpersec_total").labels(topic, partition).get().toLong, 0)
    assertEquals(gauges("broker_producer_messagesinpersec_total").labels(clientId1, topic, partition).get().toLong, 0)

    // Remove metrics and check consumer metrics
    prometheusMetricsServlet.clearPreviousMetrics()
    assertEquals(gauges("partition_log_endoffset").labels(topic, partition).get().toLong, 0)
    assertEquals(gauges("group_committed_offset").labels(groupId, topic, partition).get().toLong, 0)
    assertEquals(gauges("group_committed_offset").labels(groupId, topic, partition2).get().toLong, 0)
  }

  private def verifyAllMetricsAreRegistered():Unit = {
    val registeredPrometheusMetrics = CollectorRegistry.defaultRegistry.metricFamilySamples()
    var counter = 0

    while (registeredPrometheusMetrics.hasMoreElements) {
      registeredPrometheusMetrics.nextElement()
      counter += 1
    }

    assertEquals(39, counter)
  }

  private def setupConsumerRelatedMock(topic: String, partition: Int, groupMetaDatas: List[OffsetFetchResponseData.OffsetFetchResponseGroup]) = {
    Mockito.doReturn(groupMetaDatas, Nil: _*).when(prometheusMetricsHandler).getValidConsumerGroups
    Mockito.doReturn(prometheusMetricsHandler, Nil: _*).when(prometheusMetricsServlet).getPrometheusMetricsHandler
  }

}