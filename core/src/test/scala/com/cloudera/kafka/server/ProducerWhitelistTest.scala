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

package com.cloudera.kafka.server

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

class ProducerWhitelistTest {

  @Test
  def testWhiteListDisabled(): Unit = {
    val producerStats: ProducerStats = new ProducerStats(100, 50000, false, "^(one.*|two.*|three.*)$", mock(classOf[KafkaMetricsGroup]))
    val tp: TopicPartition = new TopicPartition("topic", 0)
    val res: Option[BrokerClientMetrics] = producerStats.clientMetrics("four", tp)
    assertTrue(res.isDefined)
  }

  @Test
  def testWhiteListEnabledEmptyWhiteList(): Unit = {
    val producerStats: ProducerStats = new ProducerStats(100, 50000, true, "", mock(classOf[KafkaMetricsGroup]))
    val tp: TopicPartition = new TopicPartition("topic", 0)
    val res: Option[BrokerClientMetrics] = producerStats.clientMetrics("four", tp)
    assertTrue(res.isDefined)
  }

  @Test
  def testWhiteListEnabledWhiteListNull(): Unit = {
    val producerStats: ProducerStats = new ProducerStats(100, 50000, true, null, mock(classOf[KafkaMetricsGroup]))
    val tp: TopicPartition = new TopicPartition("topic", 0)
    val res: Option[BrokerClientMetrics] = producerStats.clientMetrics("four", tp)
    assertTrue(res.isDefined)
  }

  @Test
  def testSinglePointMatch(): Unit = {
    val producerStats: ProducerStats = new ProducerStats(100, 50000, true, "one", mock(classOf[KafkaMetricsGroup]))
    val tp: TopicPartition = new TopicPartition("topic", 0)
    val res: Option[BrokerClientMetrics] = producerStats.clientMetrics("one", tp)
    assertTrue(res.isDefined)
  }

  @Test
  def testWholeWordMatches(): Unit = {
    val producerStats: ProducerStats = new ProducerStats(100, 50000, true, "^(one|two|three)$", mock(classOf[KafkaMetricsGroup]))
    val tp: TopicPartition = new TopicPartition("topic", 0)
    val res: Option[BrokerClientMetrics] = producerStats.clientMetrics("one", tp)
    assertTrue(res.isDefined)
  }

  @Test
  def testMatchesIfBeginsWithTheSameWord(): Unit = {
    val producerStats: ProducerStats = new ProducerStats(100, 50000, true, "^(one.*|two.*|three.*)$", mock(classOf[KafkaMetricsGroup]))
    val tp: TopicPartition = new TopicPartition("topic", 0)
    val res: Option[BrokerClientMetrics] = producerStats.clientMetrics("one1", tp)
    assertTrue(res.isDefined)
  }

  @Test
  def testMatchesWithTrimming(): Unit = {
    val producerStats: ProducerStats = new ProducerStats(100, 50000, true, "\"^(one.*|two.*|three.*)$\"", mock(classOf[KafkaMetricsGroup]))
    val tp: TopicPartition = new TopicPartition("topic", 0)
    val res: Option[BrokerClientMetrics] = producerStats.clientMetrics("one", tp)
    assertTrue(res.isDefined)
  }

  @Test
  def testDoesNotMatch(): Unit = {
    val producerStats: ProducerStats = new ProducerStats(100, 50000, true, "^(one.*|two.*|three.*)$", mock(classOf[KafkaMetricsGroup]))
    val tp: TopicPartition = new TopicPartition("topic", 0)
    val res: Option[BrokerClientMetrics] = producerStats.clientMetrics("four", tp)
    assertFalse(res.isDefined)
  }
}
