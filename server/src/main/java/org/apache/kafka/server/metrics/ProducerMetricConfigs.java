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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class ProducerMetricConfigs extends AbstractConfig {

    public static final boolean PRODUCER_METRICS_ENABLE_DEFAULT = false;
    public static final int PRODUCER_METRICS_CACHE_MAX_SIZE_DEFAULT = 1000 * 50; // producers x partitions per producer
    public static final long PRODUCER_METRICS_CACHE_ENTRY_EXPIRY_MS_DEFAULT = 70L * 60L * 1000L;

    public static final String PRODUCER_METRICS_ENABLE_CONFIG = "producer.metrics.enable";
    public static final String PRODUCER_METRICS_CACHE_MAX_SIZE_CONFIG = "producer.metrics.cache.max.size";
    public static final String PRODUCER_METRICS_CACHE_ENTRY_EXPIRY_MS_CONFIG = "producer.metrics.cache.entry.expiration.ms";
    public static final String PRODUCER_WHITELIST_ENABLED_CONFIG = "producer.metrics.whitelist.enabled";
    public static final String PRODUCER_WHITELIST_CONFIG = "producer.metrics.whitelist";

    /** ********* Producer Metrics Configuration ********** */
    public static final String PRODUCER_METRICS_ENABLE_DOC = "Enables capturing producer metrics and cache them.";
    public static final String PRODUCER_METRICS_CACHE_MAX_SIZE_DOC = "Maximum size of the (producer -> topic-partition) metric cache in a broker. When it reaches maximum size it " +
        "invalidates some existing entries that were not used often or were not used for a long time. Note: size based eviction can start even before the " +
        "cache size reaches the maximum.";
    public static final String PRODUCER_METRICS_CACHE_ENTRY_EXPIRY_MS_DOC = "Maximum time of an entry that can exist in the cache before it is accessed from or after it is written into.";
    public static final String PRODUCER_WHITELIST_ENABLED_DOC = "Enables filtering producer metrics based on clientId and the content of the whitelist";
    public static final String PRODUCER_WHITELIST_DOC = "Whitelist that contains a pattern for the producer clientIds that should be cached. Gets compiled into java.util.Pattern";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(PRODUCER_METRICS_ENABLE_CONFIG, BOOLEAN, PRODUCER_METRICS_ENABLE_DEFAULT, LOW, PRODUCER_METRICS_ENABLE_DOC)
        .define(PRODUCER_METRICS_CACHE_MAX_SIZE_CONFIG, INT, PRODUCER_METRICS_CACHE_MAX_SIZE_DEFAULT, atLeast(1), LOW, PRODUCER_METRICS_CACHE_MAX_SIZE_DOC)
        .define(PRODUCER_METRICS_CACHE_ENTRY_EXPIRY_MS_CONFIG, LONG, PRODUCER_METRICS_CACHE_ENTRY_EXPIRY_MS_DEFAULT, atLeast(1), LOW, PRODUCER_METRICS_CACHE_ENTRY_EXPIRY_MS_DOC)
        .define(PRODUCER_WHITELIST_ENABLED_CONFIG, BOOLEAN, false, LOW, PRODUCER_WHITELIST_ENABLED_DOC)
        .define(PRODUCER_WHITELIST_CONFIG, STRING, null, LOW, PRODUCER_WHITELIST_DOC);

    public ProducerMetricConfigs(Properties props) {
        super(CONFIG_DEF, props);
    }

    public static ConfigDef configDef() {
        return CONFIG_DEF;
    }

    public static Set<String> names() {
        return CONFIG_DEF.names();
    }
}
