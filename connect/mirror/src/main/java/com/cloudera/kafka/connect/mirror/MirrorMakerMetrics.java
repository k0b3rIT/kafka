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
package com.cloudera.kafka.connect.mirror;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;

import com.cloudera.kafka.connect.mirror.utils.RetryStatus;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class MirrorMakerMetrics implements AutoCloseable {

    private static final String HERDER_GROUP = "distributed-herders";

    private static final Set<String> HERDER_TAGS = new HashSet<>(Arrays.asList("herder"));

    private static final MetricNameTemplate HERDER_STATUS = new MetricNameTemplate(
            "herder-status", HERDER_GROUP,
            "Status of distributed herder", HERDER_TAGS);
    private static final MetricNameTemplate HERDER_STATUS_CODE = new MetricNameTemplate(
            "herder-status-code", HERDER_GROUP,
            "Status code of distributed herder", HERDER_TAGS);
    private static final MetricNameTemplate HERDER_REST_URL = new MetricNameTemplate(
            "herder-rest-url", HERDER_GROUP,
            "URL of distributed Herder REST endpoint", HERDER_TAGS);


    private final Metrics metrics = new Metrics();
    private final Map<String, HerderMetrics> herderMetrics = new ConcurrentHashMap<>();

    public MirrorMakerMetrics(MirrorMakerConfig config) {
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.connect.mirror");
        JmxReporter reporter = new JmxReporter();
        reporter.contextChange(metricsContext);
        metrics.addReporter(reporter);
    }

    public void herderStatus(String herder, Supplier<RetryStatus> statusSupplier) {
        herder(herder).addHerderValue(HERDER_STATUS, statusSupplier);
        herder(herder).addHerderValue(HERDER_STATUS_CODE, () -> statusSupplier.get().getStatusCode());
    }

    public void herderStatus(String herder, int n, Supplier<RetryStatus> statusSupplier) {
        MetricNameTemplate herderStatusN = new MetricNameTemplate(
                "herder-status-" + n, HERDER_GROUP,
                String.format("Last %dth status of distributed herders", n), HERDER_TAGS);
        herder(herder).addHerderValue(herderStatusN, statusSupplier);
    }

    public void removeHerderMetrics(String herder) {
        HerderMetrics metricsOfHerder = herderMetrics.get(herder);
        if (metricsOfHerder != null) {
            metricsOfHerder.removeHerderMetrics();
        }
    }

    public void herderUrl(String herder, String url) {
        herder(herder).addHerderImmutableValue(HERDER_REST_URL, url);
    }

    void removeHerderUrl(String herder) {
        herder(herder).removeHerderImmutableValue(HERDER_REST_URL);
    }

    public HerderMetrics herder(String herder) {
        return herderMetrics.computeIfAbsent(herder, HerderMetrics::new);
    }

    @Override
    public void close() {
        metrics.close();
    }

    private class HerderMetrics {
        private final Set<MetricName> registry = new HashSet<>();
        private final Map<String, String> tags = new LinkedHashMap<>();

        HerderMetrics(String herder) {
            tags.put("herder", herder);
        }

        <T> void addHerderValue(MetricNameTemplate template, Supplier<T> valueSupplier) {
            MetricName metricName = metrics.metricInstance(template, tags);
            registry.add(metricName);
            metrics.addMetric(
                    metricName,
                    (Gauge<T>) (config, now) -> valueSupplier.get());
        }

        <T> void addHerderImmutableValue(MetricNameTemplate template, final T value) {
            MetricName metricName = metrics.metricInstance(template, tags);
            registry.add(metricName);
            metrics.addMetric(
                    metrics.metricInstance(template, tags),
                    (Gauge<T>) (config, now) -> value);
        }

        void removeHerderImmutableValue(MetricNameTemplate template) {
            MetricName metricName = metrics.metricInstance(template, tags);
            registry.remove(metricName);
            metrics.removeMetric(metricName);
        }

        void removeHerderMetrics() {
            registry.stream().forEach(metrics::removeMetric);
        }


    }

}
