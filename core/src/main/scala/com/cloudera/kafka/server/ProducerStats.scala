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

import org.apache.kafka.common.TopicPartition
import kafka.utils.Logging

import java.util.regex.Pattern
import scala.ref.WeakReference
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache, RemovalCause, RemovalListener}
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util.concurrent.{Executors, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import java.util.Objects
import scala.jdk.CollectionConverters._

class ProducerStats(producerCacheMaxSize: Int, producerCacheExpiryMs: Long,
                    producerWhiteListEnabled: Boolean, producerWhiteList: String, metricsGroup: KafkaMetricsGroup) extends Logging {
  Objects.requireNonNull(producerCacheMaxSize, "producerCacheMaxSize can not be null")
  Objects.requireNonNull(producerCacheExpiryMs, "producerCacheExpiryMs can not be null")

  private val whiteList = if (producerWhiteList == null) null else getWhiteList(producerWhiteList)
  private val whiteListEnabled = producerWhiteListEnabled && whiteList != null

  private val removalListener = new RemovalListener[(String, TopicPartition), BrokerClientMetrics] with Logging {
    override def onRemoval(key: (String, TopicPartition), value: BrokerClientMetrics, cause: RemovalCause): Unit = {
      debug(s"Cache removal listener invoked for key: $key and value as $value because: $cause")
      if (value != null) {
        value.close()
      }
    }
  }

  private val cacheLoader = new CacheLoader[(String, TopicPartition), BrokerClientMetrics]() {
    override def load(key: (String, TopicPartition)): BrokerClientMetrics = new BrokerClientMetrics(key._1, key._2)
  }

  private val clientTopicPartitionToMetrics: LoadingCache[(String, TopicPartition), BrokerClientMetrics] = Caffeine.newBuilder()
    .expireAfterAccess(producerCacheExpiryMs, TimeUnit.MILLISECONDS)
    .maximumSize(producerCacheMaxSize)
    .initialCapacity(producerCacheMaxSize / 2)
    .removalListener(removalListener)
    .recordStats()
    .build(cacheLoader)

  metricsGroup.newGauge("HitCount", () => clientTopicPartitionToMetrics.stats().hitCount())
  metricsGroup.newGauge("HitRate", () => clientTopicPartitionToMetrics.stats().hitRate())
  metricsGroup.newGauge("MissCount", () => clientTopicPartitionToMetrics.stats().missCount())
  metricsGroup.newGauge("MissRate", () => clientTopicPartitionToMetrics.stats().missRate())
  metricsGroup.newGauge("TotalRequestCount", () => clientTopicPartitionToMetrics.stats().requestCount())
  metricsGroup.newGauge("LoadSuccessCount", () => clientTopicPartitionToMetrics.stats().loadSuccessCount())
  metricsGroup.newGauge("LoadFailureCount", () => clientTopicPartitionToMetrics.stats().loadFailureCount())
  metricsGroup.newGauge("LoadFailureRate", () => clientTopicPartitionToMetrics.stats().loadFailureRate())
  metricsGroup.newGauge("TotalLoadCount", () => clientTopicPartitionToMetrics.stats().loadCount())
  metricsGroup.newGauge("AverageLoadPenalty", () => clientTopicPartitionToMetrics.stats().averageLoadPenalty())
  metricsGroup.newGauge("EvictionCount", () => clientTopicPartitionToMetrics.stats().evictionCount())

  // clears cache every 5 mins as caffeine cache does not guarantee to remove entries as soon as expired.
  // This gives deterministic behavior about producers considered to be inactive as those metrics are removed.
  val threadPool = new ScheduledThreadPoolExecutor(1, new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = Executors.defaultThreadFactory().newThread(r)
      thread.setName("client-topic-metrics-cache-cleanup-thread")
      thread.setDaemon(true)
      thread
    }
  })
  threadPool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
  threadPool.scheduleWithFixedDelay(
    new CacheCleaner(new WeakReference[LoadingCache[_, _]](clientTopicPartitionToMetrics), threadPool),
    5, 300, TimeUnit.SECONDS)

  def clientMetrics(clientId: String, topicPartition: TopicPartition): Option[BrokerClientMetrics] = {
    if (whiteListEnabled && !whiteList.matcher(clientId).matches()) {
      return None
    }
    Objects.requireNonNull(topicPartition, "topicPartition can not be null")
    Some(clientTopicPartitionToMetrics.get((clientId, topicPartition)))
  }

  def getAllClientRelations(): scala.collection.Set[(String, TopicPartition)] = {
    clientTopicPartitionToMetrics.asMap().keySet().asScala
  }

  private def getWhiteList(whiteListString: String): Pattern = {
    val trimmedWhiteList = whiteListString.stripPrefix("\"").stripSuffix("\"").trim
    if (trimmedWhiteList.isEmpty) {
      null
    } else {
      Pattern.compile(trimmedWhiteList)
    }
  }

  def close(): Unit = {
    clientTopicPartitionToMetrics.invalidateAll()
  }
}
