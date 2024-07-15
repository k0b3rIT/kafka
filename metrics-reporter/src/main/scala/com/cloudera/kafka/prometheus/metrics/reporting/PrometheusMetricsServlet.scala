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

import com.cloudera.kafka.prometheus.metrics.reporting.JmxMetricNames._
import com.yammer.metrics.core._
import io.prometheus.client.exporter.MetricsServlet
import io.prometheus.client.{Gauge => PrometheusGauge}
import kafka.common.OffsetAndMetadata
import kafka.coordinator.group.GroupSummary
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.metrics.KafkaYammerMetrics

import java.util
import java.util.concurrent.ConcurrentHashMap
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.util.Try

class PrometheusMetricsServlet(prometheusMetricsHandler: PrometheusMetricsHandler) extends MetricsServlet with MetricsRegistryListener with Logging {

  private val mBeanNames: util.Map[String, Long] = new ConcurrentHashMap[String, Long]
  private val FIRST_INJECT = 0
  private val registry = KafkaYammerMetrics.defaultRegistry()


  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val updateCache = Try(req.getParameter(PrometheusMetricsServlet.UPDATE).toBoolean).getOrElse(false)

    clearPreviousMetrics()
    updatePrometheusMetrics(updateCache)
    super.doGet(req, resp)
  }

  def getPrometheusMetricsHandler: PrometheusMetricsHandler = prometheusMetricsHandler

  def getPrometheusGauges: Map[String, PrometheusGauge] = {
    getPrometheusMetricsHandler.getAllGauges
  }

  def clearPreviousMetrics(): Unit = {
    getPrometheusMetricsHandler.getAllGauges.values.foreach(_.clear())
  }

  def updatePrometheusMetrics(updateCache: Boolean): Unit = {
    processKafkaMetrics(updateCache)
    getPrometheusMetricsHandler.processConsumerGroupData()
  }

  def processKafkaMetrics(updateCache: Boolean): Unit = {
    val iter = this.registry.groupedMetrics().entrySet().iterator()

    while(iter.hasNext) {
      val entry = iter.next()

      val innerIter = entry.getValue.entrySet().iterator()

      while(innerIter.hasNext) {
        val subEntry = innerIter.next()

        if (isAllowedMetrics(subEntry.getKey)) {
          subEntry.getValue match {
            case meter: Metered => processMetered(subEntry.getKey, meter, updateCache)
            case histogram: Histogram => getPrometheusMetricsHandler.handleKafkaMetric(subEntry.getKey, histogram.getSnapshot.get99thPercentile())
            case gauge: Gauge[_] if gauge.value().isInstanceOf[Int] => getPrometheusMetricsHandler.handleKafkaMetric(subEntry.getKey, gauge.value().asInstanceOf[Int])
            case gauge: Gauge[_] if gauge.value().isInstanceOf[Long] => getPrometheusMetricsHandler.handleKafkaMetric(subEntry.getKey, gauge.value().asInstanceOf[Long].toDouble)
            case gauge: Gauge[_] if gauge.value().isInstanceOf[Double] => getPrometheusMetricsHandler.handleKafkaMetric(subEntry.getKey, gauge.value().asInstanceOf[Double])
            case _ => debug(s"Ignoring ${subEntry.getKey} Class: ${subEntry.getValue.getClass} from metrics processing")
          }
        }
      }
    }
  }

  private def processMetered(name: MetricName, meter: Metered, updateCache: Boolean): Unit = {
    handleIncreasedCount(name, meter, updateCache)
    getPrometheusMetricsHandler.handleKafkaMetric(name, meter.fifteenMinuteRate())
  }

  private def handleIncreasedCount(name: MetricName, meter: Metered, updateCache: Boolean): Unit = {
    if (name.getMBeanName != null && mBeanNames.containsKey(name.getMBeanName)) {
      val currentCount = meter.count()
      val previousCount = if (updateCache) mBeanNames.put(name.getMBeanName, currentCount) else mBeanNames.get(name.getMBeanName)

      val difference = currentCount - previousCount
      // Counters are always increasing, therefore increase amount should never be negative
      val increase = (if (difference < 0) currentCount else difference).toDouble

      trace(s"Handling Prometheus increased count. Metric name: ${name.getMBeanName} updateCache: $updateCache currentCount: $currentCount previousCount: $previousCount")
      getPrometheusMetricsHandler.handleIncreasedCount(name, increase)
    }
  }

  override def onMetricAdded(name: MetricName, metric: Metric): Unit = {
    if (isIncreaseCounterMetrics(name))
      mBeanNames.put(name.getMBeanName, FIRST_INJECT)
  }

  override def onMetricRemoved(name: MetricName): Unit = {
    if (isIncreaseCounterMetrics(name))
      mBeanNames.remove(name.getMBeanName)
  }

  private def isIncreaseCounterMetrics(name: MetricName): Boolean = {
    if (KafkaServerGroup == name.getGroup &&
      (BrokerTopicMetrics == name.getType) &&
      (BytesInPerSec == name.getName || BytesOutPerSec == name.getName|| MessagesInPerSec == name.getName)) {
      true
    } else if (KafkaServerGroup == name.getGroup &&
      (BrokerClientMetrics == name.getType) &&
      (MessagesInPerSec == name.getName)) {
      true
    } else if (KafkaControllerGroup == name.getGroup &&
      (ControllerStats == name.getType) &&
      (UncleanLeaderElectionsPerSec == name.getName)) {
      true
    } else {
      false
    }
  }

  private def isAllowedMetrics(name: MetricName): Boolean = {
    import com.cloudera.kafka.prometheus.metrics.reporting.JmxMetricNames._

    name.getGroup match {
      case KafkaServerGroup =>
        name.getType match {
          case BrokerTopicMetrics if Set(BytesInPerSec, BytesOutPerSec, MessagesInPerSec,
            TotalProduceRequestsPerSec, TotalFetchRequestsPerSec).contains(name.getName) => true
          case BrokerClientMetrics if name.getName == MessagesInPerSec => true
          case KafkaRequestHandlerPool if name.getName == RequestHandlerAvgIdlePercent => true
          case SessionExpireListener if name.getName == ZooKeeperExpiresPerSec => true
          case ReplicaManager if Set(IsrShrinksPerSec, PartitionCount,LeaderCount, UnderReplicatedPartitions).contains(name.getName) => true
          case _ => false
        }
      case KafkaControllerGroup =>
        name.getType match {
          case ControllerStats if Set(UncleanLeaderElectionsPerSec, LeaderElectionRateAndTimeMs).contains(name.getName) => true
          case KafkaController if Set(ActiveControllerCount, OfflinePartitionsCount).contains(name.getName) => true
          case _ => false
        }
      case KafkaNetworkGroup =>
        name.getType match {
          case SocketServer if name.getName == NetworkProcessorAvgIdlePercent => true
          case RequestMetrics if name.getName == TotalTimeMs && (name.getScope == RequestFetchConsumer || name.getScope == RequestProduce) => true
          case _ => false
        }
      case KafkaClusterGroup =>
        name.getType match {
          case PartitionType if Set(ReplicasCount, InSyncReplicasCount, UnderReplicated).contains(name.getName) => true
          case _ => false
        }
      case KafkaLog =>
        name.getType match {
          case Log if name.getName == LogEndOffset => true
          case _ => false
        }
      case _ => false
    }
  }

}

object PrometheusMetricsServlet {
  private val UPDATE = "update"
}

case class GroupMetaData(groupId: String, summary: GroupSummary, allOffsets: Map[TopicPartition, OffsetAndMetadata])
