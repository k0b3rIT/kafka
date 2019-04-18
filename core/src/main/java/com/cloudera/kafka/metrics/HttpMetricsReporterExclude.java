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

package com.cloudera.kafka.metrics;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

public class HttpMetricsReporterExclude implements Reconfigurable, MetricsReporterExclude {
    public static final HttpMetricsReporterExclude INSTANCE = new HttpMetricsReporterExclude();
    public static final String CONFIG_NAME = "http.metrics.exclude";

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Pattern pattern;

    private HttpMetricsReporterExclude() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            pattern = compileRegex((String) configs.get(CONFIG_NAME));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Collections.singleton(CONFIG_NAME);
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        compileRegex((String) configs.get(CONFIG_NAME));
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        configure(configs);
    }

    @Override
    public boolean shouldBeExcluded(Object metricName) {
        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            if (pattern == null) {
                return false;
            }
            return pattern.matcher((String) metricName).matches();
        } finally {
            lock.unlock();
        }
    }

    private static Pattern compileRegex(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return null;
        }

        try {
            return Pattern.compile(pattern);
        } catch (Exception e) {
            throw new ConfigException("Topic Replication Metrics Filtering config key's " + CONFIG_NAME + " value does " +
                    "not contain a valid regex!", e);
        }
    }
}