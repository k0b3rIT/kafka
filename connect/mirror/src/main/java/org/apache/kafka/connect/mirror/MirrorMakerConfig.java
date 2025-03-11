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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.CaseInsensitiveValidString.in;

/** Top-level config describing replication flows between multiple Kafka clusters.
 *  <p>
 *  Supports cluster-level properties of the form cluster.x.y.z, and replication-level
 *  properties of the form source->target.x.y.z.
 *  e.g.
 *
 * <pre>
 *      clusters = A, B, C
 *      A.bootstrap.servers = aaa:9092
 *      A.security.protocol = SSL
 *      --->%---
 *      A->B.enabled = true
 *      A->B.producer.client.id = "A-B-producer"
 *      --->%---
 * </pre>
 */
public class MirrorMakerConfig extends AbstractConfig {

    public static final String CLUSTERS_CONFIG = "clusters";
    private static final String CLUSTERS_DOC = "List of cluster aliases.";
    public static final String CONFIG_PROVIDERS_CONFIG = WorkerConfig.CONFIG_PROVIDERS_CONFIG;
    private static final String CONFIG_PROVIDERS_DOC = "Names of ConfigProviders to use.";
    public static final String REST_HOST_NAME_CONFIG = "mm.rest.host.name";
    private static final String REST_HOST_NAME_DOC = "The host name of the Connect REST servers to bind on";
    public static final String REST_PROTOCOL_CONFIG = "mm.rest.protocol";
    private static final String REST_PROTOCOL_DOC = "The protocol to be used by the Connect REST servers, HTTP or HTTPS";
    private static final String REST_PROTOCOL_DEFAULT = "http";
    public static final String REST_SERVER_LEGACY_MODE_CONFIG = "mm.rest.server.legacy.mode";
    private static final boolean REST_SERVER_LEGACY_MODE_DEFAULT = false;
    private static final String REST_SERVER_LEGACY_MODE_DOC = "Enables legacy REST server mode." +
            " (this mode used only during the rolling upgrade phase, set automatically by the CM upgrade handler, do not enable it manually).";
    public static final String MM_METRICS_SERVLET_ENABLE = "mm.metrics.servlet.enable";
    private static final String MM_METRICS_SERVLET_ENABLE_DOC = "Enable the metrics servlet for MM2";
    private static final Boolean MM_METRICS_SERVLET_ENABLE_DEFAULT = Boolean.FALSE;

    public static final String HERDER_RESTART_NUM_CONFIG = "mm.replication.restart.count";
    private static final String HERDER_RESTART_NUM_DOC = "The number of times, the failing start of a replication flow is attempted. (-1 means no upper limit)";
    private static final Integer HERDER_RESTART_NUM_DEFAULT = 1;
    public static final String HERDER_RESTART_DELAY_CONFIG = "mm.replication.restart.delay.ms";
    private static final String HERDER_RESTART_DELAY_DOC = "The delay in ms between two replication flow restart attempts.";
    private static final Long HERDER_RESTART_DELAY_DEFAULT = 5000L;

    private static final String NAME = "name";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String SOURCE_CLUSTER_ALIAS = "source.cluster.alias";
    private static final String TARGET_CLUSTER_ALIAS = "target.cluster.alias";
    private static final String GROUP_ID_CONFIG = "group.id";
    private static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";
    private static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";
    private static final String HEADER_CONVERTER_CLASS_CONFIG = "header.converter";
    private static final String BYTE_ARRAY_CONVERTER_CLASS =
        "org.apache.kafka.connect.converters.ByteArrayConverter";

    static final String SOURCE_CLUSTER_PREFIX = "source.cluster.";
    static final String TARGET_CLUSTER_PREFIX = "target.cluster.";
    static final String SOURCE_PREFIX = "source.";
    static final String TARGET_PREFIX = "target.";
    static final String ENABLE_INTERNAL_REST_CONFIG = "dedicated.mode.enable.internal.rest";
    private static final String ENABLE_INTERNAL_REST_DOC =
            "Whether to bring up an internal-only REST server that allows multi-node clusters to operate correctly.";
    static final String GLOBAL_WORKER_CONFIG_PREFIX = "workers.";
    static final String GLOBAL_CONNECTOR_CONFIG_PREFIX = "connectors.";

    private final Plugins plugins;

    private final Map<String, String> rawProperties;

    @SuppressWarnings("this-escape")
    public MirrorMakerConfig(Map<String, String> props) {
        super(config(), props, true);
        plugins = new Plugins(originalsStrings());

        rawProperties = new HashMap<>(props);
    }

    public Set<String> clusters() {
        return new HashSet<>(getList(CLUSTERS_CONFIG));
    }

    public boolean enableInternalRest() {
        return getBoolean(ENABLE_INTERNAL_REST_CONFIG);
    }

    public boolean legacyRestServerModeEnabled() {
        return getBoolean(REST_SERVER_LEGACY_MODE_CONFIG);
    }

    public List<SourceAndTarget> clusterPairs() {
        List<SourceAndTarget> pairs = new ArrayList<>();
        Set<String> clusters = clusters();
        Map<String, String> originalStrings = originalsStrings();
        boolean globalHeartbeatsEnabled = MirrorHeartbeatConfig.EMIT_HEARTBEATS_ENABLED_DEFAULT;
        if (originalStrings.containsKey(MirrorHeartbeatConfig.EMIT_HEARTBEATS_ENABLED)) {
            globalHeartbeatsEnabled = Boolean.parseBoolean(originalStrings.get(MirrorHeartbeatConfig.EMIT_HEARTBEATS_ENABLED));
        }

        for (String source : clusters) {
            for (String target : clusters) {
                if (!source.equals(target)) {
                    String clusterPairConfigPrefix = source + "->" + target + ".";
                    boolean clusterPairEnabled = Boolean.parseBoolean(originalStrings.get(clusterPairConfigPrefix + "enabled"));
                    boolean clusterPairHeartbeatsEnabled = globalHeartbeatsEnabled;
                    if (originalStrings.containsKey(clusterPairConfigPrefix + MirrorHeartbeatConfig.EMIT_HEARTBEATS_ENABLED)) {
                        clusterPairHeartbeatsEnabled = Boolean.parseBoolean(originalStrings.get(clusterPairConfigPrefix + MirrorHeartbeatConfig.EMIT_HEARTBEATS_ENABLED));
                    }

                    // By default, all source->target Herder combinations are created even if `x->y.enabled=false`
                    // Unless `emit.heartbeats.enabled=false` or `x->y.emit.heartbeats.enabled=false`
                    // Reason for this behavior: for a given replication flow A->B with heartbeats, 2 herders are required :
                    // B->A for the MirrorHeartbeatConnector (emits heartbeats into A for monitoring replication health)
                    // A->B for the MirrorSourceConnector (actual replication flow)
                    if (clusterPairEnabled || clusterPairHeartbeatsEnabled) {
                        pairs.add(new SourceAndTarget(source, target));
                    }
                }
            }
        }
        return pairs;
    }

    /** Construct a MirrorClientConfig from properties of the form cluster.x.y.z.
      * Use to connect to a cluster based on the MirrorMaker top-level config file.
      */
    public MirrorClientConfig clientConfig(String cluster) {
        Map<String, String> props = new HashMap<>();
        props.putAll(originalsStrings());
        props.putAll(clusterProps(cluster));
        return new MirrorClientConfig(transform(props));
    }

    // loads properties of the form cluster.x.y.z
    Map<String, String> clusterProps(String cluster) {
        Map<String, String> props = new HashMap<>();

        props.putAll(stringsWithPrefixStripped(cluster + "."));

        for (String k : MirrorClientConfig.CLIENT_CONFIG_DEF.names()) {
            String v = props.get(k);
            if (v != null) {
                props.putIfAbsent("producer." + k, v);
                props.putIfAbsent("consumer." + k, v);
                props.putIfAbsent("admin." + k, v);
            }
        }

        for (String k : MirrorClientConfig.CLIENT_CONFIG_DEF.names()) {
            String v = rawProperties.get(k);
            if (v != null) {
                props.putIfAbsent("producer." + k, v);
                props.putIfAbsent("consumer." + k, v);
                props.putIfAbsent("admin." + k, v);
                props.putIfAbsent(k, v);
            }
        }
 
        return props;
    }

    // loads worker configs based on properties of the form x.y.z and cluster.x.y.z 
    public Map<String, String> workerConfig(SourceAndTarget sourceAndTarget) {
        Map<String, String> props = new HashMap<>();
        props.putAll(clusterProps(sourceAndTarget.target()));

        // Accept common top-level configs that are otherwise ignored by MM2.
        // N.B. all other worker properties should be configured for specific herders,
        // e.g. primary->backup.client.id
        props.putAll(stringsWithPrefix("offset.storage"));
        props.putAll(stringsWithPrefix("config.storage"));
        props.putAll(stringsWithPrefix("status.storage"));
        props.putAll(stringsWithPrefix("key.converter")); 
        props.putAll(stringsWithPrefix("value.converter")); 
        props.putAll(stringsWithPrefix("header.converter"));
        props.putAll(stringsWithPrefix("task"));
        props.putAll(stringsWithPrefix("worker"));
        props.putAll(stringsWithPrefix("replication.policy"));

        // Other global worker configs
        props.putAll(stringsWithPrefixStripped(GLOBAL_WORKER_CONFIG_PREFIX));

        // Other global worker configs
        props.putAll(stringsWithPrefixStripped(GLOBAL_WORKER_CONFIG_PREFIX));

        props.putAll(stringsWithPrefix("listeners.https."));

        // Per-worker overrides
        props.putAll(stringsWithPrefixStripped(sourceAndTarget.source() + "->"
            + sourceAndTarget.target() + ".worker."));

        // transform any expression like ${provider:path:key}, since the worker doesn't do so
        props = transform(props);
        props.putAll(stringsWithPrefix(CONFIG_PROVIDERS_CONFIG));

        // fill in reasonable defaults
        props.putIfAbsent(CommonClientConfigs.CLIENT_ID_CONFIG, sourceAndTarget.toString());
        props.putIfAbsent(GROUP_ID_CONFIG, sourceAndTarget.source() + "-mm2");
        props.putIfAbsent(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "mm2-offsets."
                + sourceAndTarget.source() + ".internal");
        props.putIfAbsent(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "mm2-status."
                + sourceAndTarget.source() + ".internal");
        props.putIfAbsent(DistributedConfig.CONFIG_TOPIC_CONFIG, "mm2-configs."
                + sourceAndTarget.source() + ".internal");
        props.putIfAbsent(KEY_CONVERTER_CLASS_CONFIG, BYTE_ARRAY_CONVERTER_CLASS); 
        props.putIfAbsent(VALUE_CONVERTER_CLASS_CONFIG, BYTE_ARRAY_CONVERTER_CLASS); 
        props.putIfAbsent(HEADER_CONVERTER_CLASS_CONFIG, BYTE_ARRAY_CONVERTER_CLASS);

        // Providing a default listener for the rest server
        // Listeners can be configured per-replication flow, but we need a sensible default
        // The MM2 level config can be used to define the hostname and protocol for all of the servers in the process
        String restListeners = props.get(RestServerConfig.LISTENERS_CONFIG);
        if (restListeners == null || restListeners.isEmpty()) {
            String hostname = getString(REST_HOST_NAME_CONFIG);
            String protocol = getString(REST_PROTOCOL_CONFIG).toLowerCase(Locale.ROOT);
            props.put(RestServerConfig.LISTENERS_CONFIG, protocol + "://" + hostname + ":0");
        }

        props.put(WorkerConfig.METRIC_GROUPNAME_POSTFIX_CONFIG, "." + sourceAndTarget.source() + "__" + sourceAndTarget.target());

        props.putIfAbsent(MirrorConnectorConfig.SOURCE_CLUSTER_ALIAS, sourceAndTarget.source());
        props.putIfAbsent(MirrorConnectorConfig.TARGET_CLUSTER_ALIAS, sourceAndTarget.target());

        return props;
    }

    Set<String> allConfigNames() {
        Set<String> allNames = new HashSet<>();
        List<ConfigDef> connectorConfigDefs = Arrays.asList(
                MirrorCheckpointConfig.CONNECTOR_CONFIG_DEF,
                MirrorSourceConfig.CONNECTOR_CONFIG_DEF,
                MirrorHeartbeatConfig.CONNECTOR_CONFIG_DEF
        );
        for (ConfigDef cd : connectorConfigDefs) {
            allNames.addAll(cd.names());
        }
        return allNames;
    }

    // loads properties of the form cluster.x.y.z and source->target.x.y.z
    public Map<String, String> connectorBaseConfig(SourceAndTarget sourceAndTarget, Class<?> connectorClass) {
        Map<String, String> props = new HashMap<>();

        props.putAll(rawProperties);
        props.keySet().retainAll(allConfigNames());
        
        props.putAll(stringsWithPrefix(CONFIG_PROVIDERS_CONFIG));
        props.putAll(stringsWithPrefix("replication.policy"));

        Map<String, String> sourceClusterProps = clusterProps(sourceAndTarget.source());
        // attrs non prefixed with producer|consumer|admin
        props.putAll(clusterConfigsWithPrefix(SOURCE_CLUSTER_PREFIX, sourceClusterProps));
        // attrs prefixed with producer|consumer|admin
        props.putAll(clientConfigsWithPrefix(SOURCE_PREFIX, sourceClusterProps));

        Map<String, String> targetClusterProps = clusterProps(sourceAndTarget.target());
        props.putAll(clusterConfigsWithPrefix(TARGET_CLUSTER_PREFIX, targetClusterProps));
        props.putAll(clientConfigsWithPrefix(TARGET_PREFIX, targetClusterProps));

        props.putIfAbsent(NAME, connectorClass.getSimpleName());
        props.putIfAbsent(CONNECTOR_CLASS, connectorClass.getName());
        props.putIfAbsent(SOURCE_CLUSTER_ALIAS, sourceAndTarget.source());
        props.putIfAbsent(TARGET_CLUSTER_ALIAS, sourceAndTarget.target());

        // global connector properties
        props.putAll(stringsWithPrefixStripped(GLOBAL_CONNECTOR_CONFIG_PREFIX));

        // override with connector-level properties
        props.putAll(stringsWithPrefixStripped(sourceAndTarget.source() + "->"
            + sourceAndTarget.target() + "."));

        // disabled by default
        props.putIfAbsent(MirrorConnectorConfig.ENABLED, "false");

        // don't transform -- the worker will handle transformation of Connector and Task configs
        return props;
    }

    List<String> configProviders() {
        return getList(CONFIG_PROVIDERS_CONFIG);
    } 

    Map<String, String> transform(Map<String, String> props) {
        // transform worker config according to config.providers
        List<String> providerNames = configProviders();
        Map<String, ConfigProvider> providers = new HashMap<>();
        for (String name : providerNames) {
            ConfigProvider configProvider = plugins.newConfigProvider(
                    this,
                    CONFIG_PROVIDERS_CONFIG + "." + name,
                    Plugins.ClassLoaderUsage.PLUGINS
            );
            providers.put(name, configProvider);
        }
        ConfigTransformer transformer = new ConfigTransformer(providers);
        Map<String, String> transformed = transformer.transform(props).data();
        providers.values().forEach(x -> Utils.closeQuietly(x, "config provider"));
        return transformed;
    }

    protected static ConfigDef config() {
        ConfigDef result = new ConfigDef()
                .define(CLUSTERS_CONFIG, Type.LIST, Importance.HIGH, CLUSTERS_DOC)
                .define(ENABLE_INTERNAL_REST_CONFIG, Type.BOOLEAN, false, Importance.HIGH, ENABLE_INTERNAL_REST_DOC)
                .define(CONFIG_PROVIDERS_CONFIG, Type.LIST, Collections.emptyList(), Importance.LOW, CONFIG_PROVIDERS_DOC)
                .define(REST_HOST_NAME_CONFIG, Type.STRING, "", Importance.MEDIUM, REST_HOST_NAME_DOC)
                .define(REST_PROTOCOL_CONFIG,
                        Type.STRING,
                        REST_PROTOCOL_DEFAULT,
                        ConfigDef.CaseInsensitiveValidString.in("http", "https"),
                        Importance.MEDIUM,
                        REST_PROTOCOL_DOC)
                .define(REST_SERVER_LEGACY_MODE_CONFIG,
                        Type.BOOLEAN,
                        REST_SERVER_LEGACY_MODE_DEFAULT,
                        Importance.LOW,
                        REST_SERVER_LEGACY_MODE_DOC)
                // security support
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        in(Utils.enumOptions(SecurityProtocol.class)),
                        Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .define(MM_METRICS_SERVLET_ENABLE,
                        Type.BOOLEAN,
                        MM_METRICS_SERVLET_ENABLE_DEFAULT,
                        Importance.LOW,
                        MM_METRICS_SERVLET_ENABLE_DOC)
                .define(HERDER_RESTART_NUM_CONFIG,
                        Type.INT,
                        HERDER_RESTART_NUM_DEFAULT,
                        Importance.LOW,
                        HERDER_RESTART_NUM_DOC)
                .define(HERDER_RESTART_DELAY_CONFIG,
                        Type.LONG,
                        HERDER_RESTART_DELAY_DEFAULT,
                        Importance.LOW,
                        HERDER_RESTART_DELAY_DOC)
                .withClientSslSupport()
                .withClientSaslSupport();
        RestServerConfig.addInternalConfig(result);
        return result;
    }

    private Map<String, String> stringsWithPrefixStripped(String prefix) {
        return Utils.entriesWithPrefix(rawProperties, prefix);
    }

    private Map<String, String> stringsWithPrefix(String prefix) {
        return Utils.entriesWithPrefix(rawProperties, prefix, false, true);
    }

    static Map<String, String> clusterConfigsWithPrefix(String prefix, Map<String, String> props) {
        return props.entrySet().stream()
                .filter(x -> !x.getKey().matches("(^consumer.*|^producer.*|^admin.*)"))
                .collect(Collectors.toMap(x -> prefix + x.getKey(), Entry::getValue));
    }

    static Map<String, String> clientConfigsWithPrefix(String prefix, Map<String, String> props) {
        return props.entrySet().stream()
                .filter(x -> x.getKey().matches("(^consumer.*|^producer.*|^admin.*)"))
                .collect(Collectors.toMap(x -> prefix + x.getKey(), Entry::getValue));
    }
}
