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
package com.yammer.metrics.reporting

import java.util
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.cloudera.kafka.metrics.HttpMetricsReporterExclude
import com.fasterxml.jackson.core.JsonGenerator
import com.yammer.metrics.core._
import kafka.utils.Logging

class CustomMetricsServlet(var registry: MetricsRegistry) extends MetricsServlet with MetricsRegistryListener with Logging {

  private val mBeanNames: util.Map[String, Integer] = new ConcurrentHashMap[String, Integer]
  private val FIRST_INJECT = 0
  private val SECOND_INJECT = 1
  private val topicReplicaMetricsFilter = HttpMetricsReporterExclude.INSTANCE

  override def writeRegularMetrics(json: JsonGenerator, classPrefix: String, showFullSamples: Boolean): Unit = {
    registry.groupedMetrics().forEach((meticClassName, metricNameToMetrics) => {
      if ((classPrefix == null || meticClassName.startsWith(classPrefix)) && !topicReplicaMetricsFilter.shouldBeExcluded(meticClassName)) {
        json.writeFieldName(meticClassName)
        json.writeStartObject()
        metricNameToMetrics.forEach((metricName, metrics) => {
          json.writeFieldName(metricName.getName)
          try {
            metrics.processWith(this, metricName, new MetricsServlet.Context(json, showFullSamples))
          } catch {
            case e: Throwable => warn(s"Error writing out $metricName while processing MetricsRequest.", e)
          }
        })
        json.writeEndObject()
      }
    })
  }

  override def processMeter(name: MetricName, meter: Metered, context: MetricsServlet.Context): Unit = {
    var injectZero = false
    if (name.getMBeanName != null && mBeanNames.containsKey(name.getMBeanName)) {
      // Injecting the zeroMeter twice so that CM can generate the first minute data point with "zero" as value.
      if (mBeanNames.remove(name.getMBeanName) == FIRST_INJECT) {
        mBeanNames.put(name.getMBeanName, SECOND_INJECT)
      }
      injectZero = true
    }
    if (injectZero) {
      super.processMeter(name, new ZeroMeteredDelegate(meter), context)
    } else {
      super.processMeter(name, meter, context)
    }
  }

  override def onMetricAdded(name: MetricName, metric: Metric): Unit = {
    if (isBrokerTopicOrClientMetrics(name))
      mBeanNames.put(name.getMBeanName, FIRST_INJECT)
  }

  override def onMetricRemoved(name: MetricName): Unit = {
    if (isBrokerTopicOrClientMetrics(name))
      mBeanNames.remove(name.getMBeanName)
  }

  private def isBrokerTopicOrClientMetrics(name: MetricName) = {
    "kafka.server" == name.getGroup &&
      ("BrokerTopicMetrics" == name.getType || "BrokerClientMetrics" == name.getType) &&
      name.hasScope
  }
}

class ZeroMeteredDelegate(delegate: Metered) extends Metered {
  override def rateUnit(): TimeUnit = delegate.rateUnit()
  override def eventType(): String = delegate.eventType()
  override def count(): Long = 0
  override def fifteenMinuteRate(): Double = 0
  override def fiveMinuteRate(): Double = 0
  override def meanRate(): Double = 0
  override def oneMinuteRate(): Double = 0
  override def processWith[T](processor: MetricProcessor[T], name: MetricName, context: T): Unit =
    delegate.processWith(processor, name, context)
}
