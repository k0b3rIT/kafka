/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.kafka.server

import com.yammer.metrics.core.Meter
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.storage.log.metrics.BrokerTopicMetrics

import java.util
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class BrokerClientMetrics(clientId: String, topicPartition: TopicPartition) extends Logging {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val tags: util.Map[String, String] =
    Map("clientId" -> clientId, "topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString).asJava

  val messagesInRate: Meter = metricsGroup.newMeter(BrokerTopicMetrics.MESSAGE_IN_PER_SEC, "messages", TimeUnit.SECONDS, tags)

  def close(): Unit = {
    metricsGroup.removeMetric(BrokerTopicMetrics.MESSAGE_IN_PER_SEC, tags)
    debug(s"Removing client metrics $tags")
  }
}
