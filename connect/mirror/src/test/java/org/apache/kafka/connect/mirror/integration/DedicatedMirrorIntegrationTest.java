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
package org.apache.kafka.connect.mirror.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMaker;
import org.apache.kafka.connect.mirror.MirrorSourceConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.distributed.RebalanceNeededException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.test.NoRetryException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriBuilder;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.connect.mirror.MirrorMaker.CONNECTOR_CLASSES;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class DedicatedMirrorIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(DedicatedMirrorIntegrationTest.class);
    private static final int TOPIC_CREATION_TIMEOUT_MS = 30_000;
    private static final int TOPIC_REPLICATION_TIMEOUT_MS = 30_000;
    private static final long MM_START_UP_TIMEOUT_MS = 120_000;
    private Map<String, EmbeddedKafkaCluster> kafkaClusters;
    private Map<String, MirrorMaker> mirrorMakers;

    @BeforeEach
    public void setup() {
        kafkaClusters = new HashMap<>();
        mirrorMakers = new HashMap<>();
    }

    @AfterEach
    public void teardown() throws Throwable {
        AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        mirrorMakers.forEach((name, mirrorMaker) ->
                Utils.closeQuietly(mirrorMaker::stop, "MirrorMaker worker '" + name + "'", shutdownFailure));
        mirrorMakers.forEach((name, mirrorMaker) -> mirrorMaker.awaitStop());
        kafkaClusters.forEach((name, kafkaCluster) ->
            Utils.closeQuietly(kafkaCluster::stop, "Embedded Kafka cluster '" + name + "'", shutdownFailure)
        );
        if (shutdownFailure.get() != null) {
            throw shutdownFailure.get();
        }
    }

    private EmbeddedKafkaCluster startKafkaCluster(String name, int numBrokers, Properties brokerProperties) {
        if (kafkaClusters.containsKey(name))
            throw new IllegalStateException("Cannot register multiple Kafka clusters with the same name");

        EmbeddedKafkaCluster result = new EmbeddedKafkaCluster(numBrokers, brokerProperties);
        kafkaClusters.put(name, result);

        result.start();

        return result;
    }

    private MirrorMaker startMirrorMaker(String name, Map<String, String> mmProps) {
        if (mirrorMakers.containsKey(name))
            throw new IllegalStateException("Cannot register multiple MirrorMaker nodes with the same name");

        MirrorMaker result = new MirrorMaker(mmProps);
        mirrorMakers.put(name, result);

        result.start();

        return result;
    }

    private void stopMirrorMaker(String name) {
        MirrorMaker mirror = mirrorMakers.remove(name);
        if (mirror == null) {
            throw new IllegalStateException("No MirrorMaker named " + name + " has been started");
        }
        mirror.stop();
        mirror.awaitStop();
    }

    /**
     * Tests a single-node cluster without the REST server enabled.
     */
    @Test
    public void testSingleNodeCluster() throws Exception {
        Properties brokerProps = new Properties();
        EmbeddedKafkaCluster clusterA = startKafkaCluster("A", 1, brokerProps);
        EmbeddedKafkaCluster clusterB = startKafkaCluster("B", 1, brokerProps);

        try (Admin adminB = clusterB.createAdminClient()) {

            // Cluster aliases
            final String a = "A";
            final String b = "B";
            final String ab = a + "->" + b;
            final String ba = b + "->" + a;
            final String testTopicPrefix = "test-topic-";

            Map<String, String> mmProps = new HashMap<String, String>() {{
                    put("dedicated.mode.enable.internal.rest", "false");
                    put("listeners", "http://localhost:0");
                    // Refresh topics very frequently to quickly pick up on topics that are created
                    // after the MM2 nodes are brought up during testing
                    put("refresh.topics.interval.seconds", "1");
                    put("clusters", String.join(", ", a, b));
                    put(a + ".bootstrap.servers", clusterA.bootstrapServers());
                    put(b + ".bootstrap.servers", clusterB.bootstrapServers());
                    put(ab + ".enabled", "true");
                    put(ab + ".topics", "^" + testTopicPrefix + ".*");
                    put(ba + ".enabled", "false");
                    put(ba + ".emit.heartbeats.enabled", "false");
                    put("replication.factor", "1");
                    put("checkpoints.topic.replication.factor", "1");
                    put("heartbeats.topic.replication.factor", "1");
                    put("offset-syncs.topic.replication.factor", "1");
                    put("offset.storage.replication.factor", "1");
                    put("status.storage.replication.factor", "1");
                    put("config.storage.replication.factor", "1");
                    put("mm.replication.restart.count", "5");
                    put("mm.replication.restart.delay.ms", "10000");
                }};

            // Bring up a single-node cluster
            final MirrorMaker mm = startMirrorMaker("single node", mmProps);
            final SourceAndTarget sourceAndTarget = new SourceAndTarget(a, b);
            awaitMirrorMakerStart(mm, sourceAndTarget);

            // wait for heartbeat connector to start a task
            awaitConnectorTasksStart(mm, MirrorHeartbeatConnector.class, sourceAndTarget);

            final int numMessages = 10;
            String topic = testTopicPrefix + "1";

            // Create the topic on cluster A
            clusterA.createTopic(topic, 1);
            // and wait for MirrorMaker to create it on cluster B
            awaitTopicCreation(b, adminB, a + "." + topic);

            // wait for source connector to start a task
            awaitConnectorTasksStart(mm, MirrorSourceConnector.class, sourceAndTarget);

            // Write data to the topic on cluster A
            writeToTopic(clusterA, topic, numMessages);
            // and wait for MirrorMaker to copy it to cluster B
            awaitTopicContent(clusterB, b, a + "." + topic, numMessages);
        }
    }

    @Test
    public void testClusterWithEmitOffsetDisabled() throws Exception {
        Properties brokerProps = new Properties();
        EmbeddedKafkaCluster clusterA = startKafkaCluster("A", 1, brokerProps);
        EmbeddedKafkaCluster clusterB = startKafkaCluster("B", 1, brokerProps);

        try (Admin adminB = clusterB.createAdminClient()) {

            // Cluster aliases
            final String a = "A";
            final String b = "B";
            final String ab = a + "->" + b;
            final String testTopicPrefix = "test-topic-";

            Map<String, String> mmProps = new HashMap<String, String>() {{
                    put("dedicated.mode.enable.internal.rest", "false");
                    put("listeners", "http://localhost:0");
                    // Refresh topics very frequently to quickly pick up on topics that are created
                    // after the MM2 nodes are brought up during testing
                    put("refresh.topics.interval.seconds", "1");
                    put("clusters", String.join(", ", a, b));
                    put(a + ".bootstrap.servers", clusterA.bootstrapServers());
                    put(b + ".bootstrap.servers", clusterB.bootstrapServers());
                    put(ab + ".enabled", "true");
                    put(ab + ".topics", "^" + testTopicPrefix + ".*");
                    put("replication.factor", "1");
                    put("checkpoints.topic.replication.factor", "1");
                    put("heartbeats.topic.replication.factor", "1");
                    put("emit.offset-syncs.enabled", "false");
                    put("status.storage.replication.factor", "1");
                    put("offset.storage.replication.factor", "1");
                    put("config.storage.replication.factor", "1");
                }};

            // Bring up a single-node cluster
            final MirrorMaker mm = startMirrorMaker("no-offset-syncing", mmProps);
            final SourceAndTarget sourceAndTarget = new SourceAndTarget(a, b);
            awaitMirrorMakerStart(mm, sourceAndTarget, Arrays.asList(MirrorSourceConnector.class, MirrorHeartbeatConnector.class));

            // wait for mirror source and heartbeat connectors to start a task
            awaitConnectorTasksStart(mm, MirrorHeartbeatConnector.class, sourceAndTarget);

            final int numMessages = 10;
            String topic = testTopicPrefix + "1";

            // Create the topic on cluster A
            clusterA.createTopic(topic, 1);
            // and wait for MirrorMaker to create it on cluster B
            awaitTopicCreation(b, adminB, a + "." + topic);

            // wait for source connector to start a task
            awaitConnectorTasksStart(mm, MirrorSourceConnector.class, sourceAndTarget);


            // Write data to the topic on cluster A
            writeToTopic(clusterA, topic, numMessages);
            // and wait for MirrorMaker to copy it to cluster B
            awaitTopicContent(clusterB, b, a + "." + topic, numMessages);

            List<TopicDescription> offsetSyncTopic = clusterA.describeTopics("mm2-offset-syncs.B.internal").values()
                    .stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());

            assertTrue(offsetSyncTopic.isEmpty());
        }
    }

    /**
     * Test that a multi-node dedicated cluster is able to dynamically detect new topics at runtime
     * and reconfigure its connectors and their tasks to replicate those topics correctly.
     * See <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-710%3A+Full+support+for+distributed+mode+in+dedicated+MirrorMaker+2.0+clusters">KIP-710</a>
     * for more detail on the necessity for this test case.
     */
    @Test
    public void testMultiNodeCluster() throws Exception {
        Properties brokerProps = new Properties();
        brokerProps.put("transaction.state.log.replication.factor", "1");
        brokerProps.put("transaction.state.log.min.isr", "1");
        EmbeddedKafkaCluster clusterA = startKafkaCluster("A", 1, brokerProps);
        EmbeddedKafkaCluster clusterB = startKafkaCluster("B", 1, brokerProps);

        try (Admin adminB = clusterB.createAdminClient()) {
            // Cluster aliases
            final String a = "A";
            // Use a convoluted cluster name to ensure URL encoding/decoding works
            final String b = "B- ._~:/?#[]@!$&'()*+;=\"<>%{}|\\^`618";
            final String ab = a + "->" + b;
            final String ba = b + "->" + a;
            final String testTopicPrefix = "test-topic-";

            Map<String, String> mmProps = new HashMap<String, String>() {{
                    put("dedicated.mode.enable.internal.rest", "true");
                    put("listeners", "http://localhost:0");
                    // Refresh topics very frequently to quickly pick up on topics that are created
                    // after the MM2 nodes are brought up during testing
                    put(MirrorSourceConfig.REFRESH_TOPICS_INTERVAL_SECONDS, "1");
                    put("clusters", String.join(", ", a, b));
                    put(a + ".bootstrap.servers", clusterA.bootstrapServers());
                    put(b + ".bootstrap.servers", clusterB.bootstrapServers());
                    // Enable exactly-once support to both validate that MirrorMaker can run with
                    // that feature turned on, and to force cross-worker communication before
                    // task startup
                    put(b + ".exactly.once.source.support", "enabled");
                    put(a + ".consumer.isolation.level", "read_committed");
                    put(ab + ".enabled", "true");
                    put(ab + ".topics", "^" + testTopicPrefix + ".*");
                    // The name of the offset syncs topic will contain the name of the cluster in
                    // the replication flow that it is _not_ hosted on; create the offset syncs topic
                    // on the target cluster so that its name will contain the source cluster's name
                    // (since the target cluster's name contains characters that are not valid for
                    // use in a topic name)
                    put(ab + ".offset-syncs.topic.location", "target");
                    // Disable b -> a (and heartbeats from it) so that no topics are created that use
                    // the target cluster's name
                    put(ba + ".enabled", "false");
                    put(ba + ".emit.heartbeats.enabled", "false");
                    put("replication.factor", "1");
                    put("checkpoints.topic.replication.factor", "1");
                    put("heartbeats.topic.replication.factor", "1");
                    put("offset-syncs.topic.replication.factor", "1");
                    put("offset.storage.replication.factor", "1");
                    put("status.storage.replication.factor", "1");
                    put("config.storage.replication.factor", "1");
                    // For the multi-node case, we wait for reassignment so shorten the delay period.
                    put(a + "." + DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, "1000");
                    put(b + "." + DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, "1000");
                    put("mm.replication.restart.count", "5");
                    put("mm.replication.restart.delay.ms", "5000");
                }};

            final SourceAndTarget sourceAndTarget = new SourceAndTarget(a, b);
            // Bring up a three-node cluster
            final int numNodes = 3;
            for (int i = 0; i < numNodes; i++) {
                startMirrorMaker("node " + i, mmProps);
            }

            // wait for mirror maker to start
            awaitMirrorMakerStart(mirrorMakers.get("node 0"), sourceAndTarget);

            // wait for heartbeat connector to start running
            awaitConnectorTasksStart(mirrorMakers.get("node 0"), MirrorHeartbeatConnector.class, sourceAndTarget);

            final int messagesPerTopic = 10;
            // Create one topic per Kafka cluster per MirrorMaker node
            for (int i = 0; i < numNodes; i++) {
                String topic = testTopicPrefix + i;

                // Create the topic on cluster A
                clusterA.createTopic(topic, 1);
                // and wait for MirrorMaker to create it on cluster B
                awaitTopicCreation(b, adminB, a + "." + topic);

                // wait for source connector to start running
                awaitConnectorTasksStart(mirrorMakers.get("node " + i), MirrorSourceConnector.class, sourceAndTarget);

                // Write data to the topic on cluster A
                writeToTopic(clusterA, topic, messagesPerTopic);
                // and wait for MirrorMaker to copy it to cluster B
                awaitTopicContent(clusterB, b, a + "." + topic, messagesPerTopic);
            }

            // Perform a rolling restart of the cluster with a new configuration
            Map<String, String> newMmProps = new HashMap<>(mmProps);
            String newConfigValue = "2";
            newMmProps.put(MirrorSourceConfig.REFRESH_TOPICS_INTERVAL_SECONDS, newConfigValue);
            for (int i = 0; i < numNodes; i++) {
                stopMirrorMaker("node " + i);
                MirrorMaker any = mirrorMakers.values().stream().findAny().get();
                // Wait for the cluster finish the reassignment and rebalance before bringing up the next node.
                awaitConnectorTasksStart(any, MirrorHeartbeatConnector.class, sourceAndTarget);
                awaitConnectorTasksStart(any, MirrorSourceConnector.class, sourceAndTarget);
                startMirrorMaker("node " + i, newMmProps);
                awaitMirrorMakerStart(mirrorMakers.get("node " + i), sourceAndTarget);
            }
            // Assert that the new configuration is propagated
            awaitTaskConfigurations(mirrorMakers.get("node 0"), MirrorSourceConnector.class, sourceAndTarget,
                    config -> newConfigValue.equals(config.get(MirrorSourceConfig.REFRESH_TOPICS_INTERVAL_SECONDS)));
        }
    }

    @Test
    public void testRestServerWithLegacyAndNewRestNodesBothActive() throws Exception {
        Properties brokerProps = new Properties();
        brokerProps.put("transaction.state.log.replication.factor", "1");
        brokerProps.put("transaction.state.log.min.isr", "1");
        EmbeddedKafkaCluster clusterA = startKafkaCluster("A", 1, brokerProps);
        EmbeddedKafkaCluster clusterB = startKafkaCluster("B", 1, brokerProps);

        try (Admin adminB = clusterB.createAdminClient()) {
            // Cluster aliases
            final String a = "A";
            // Use a convoluted cluster name to ensure URL encoding/decoding works
            final String b = "B";
            final String ab = a + "->" + b;
            final String ba = b + "->" + a;
            final String testTopicPrefix = "test-topic-";

            Map<String, String> mmProps = new HashMap<String, String>() {{
                    put("dedicated.mode.enable.internal.rest", "true");
                    put("listeners", "http://localhost:0");
                    // Refresh topics very frequently to quickly pick up on topics that are created
                    // after the MM2 nodes are brought up during testing
                    put(MirrorSourceConfig.REFRESH_TOPICS_INTERVAL_SECONDS, "1");
                    put("clusters", String.join(", ", a, b));
                    put(a + ".bootstrap.servers", clusterA.bootstrapServers());
                    put(b + ".bootstrap.servers", clusterB.bootstrapServers());
                    // Enable exactly-once support to both validate that MirrorMaker can run with
                    // that feature turned on, and to force cross-worker communication before
                    // task startup
                    put(b + ".exactly.once.source.support", "enabled");
                    put(a + ".consumer.isolation.level", "read_committed");
                    put(ab + ".enabled", "true");
                    put(ab + ".topics", "^" + testTopicPrefix + ".*");
                    // The name of the offset syncs topic will contain the name of the cluster in
                    // the replication flow that it is _not_ hosted on; create the offset syncs topic
                    // on the target cluster so that its name will contain the source cluster's name
                    // (since the target cluster's name contains characters that are not valid for
                    // use in a topic name)
                    put(ab + ".offset-syncs.topic.location", "target");
                    // Disable b -> a (and heartbeats from it) so that no topics are created that use
                    // the target cluster's name
                    put(ba + ".enabled", "false");
                    put(ba + ".emit.heartbeats.enabled", "false");
                    put("replication.factor", "1");
                    put("checkpoints.topic.replication.factor", "1");
                    put("heartbeats.topic.replication.factor", "1");
                    put("offset-syncs.topic.replication.factor", "1");
                    put("offset.storage.replication.factor", "1");
                    put("status.storage.replication.factor", "1");
                    put("config.storage.replication.factor", "1");
                    // For the multi-node case, we wait for reassignment so shorten the delay period.
                    put(a + "." + DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, "1000");
                    put(b + "." + DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, "1000");
                    put("mm.replication.restart.count", "5");
                    put("mm.replication.restart.delay.ms", "5000");
                    put("mm.rest.server.legacy.mode", "true");
                    put("mm.rest.host.name", "localhost");
                }};

            final int numNodes = 3;
            HashMap<Integer, Map<String, String>> perNodeProps = new HashMap<>();
            for (int i = 0; i < numNodes; i++) {
                perNodeProps.put(i, new HashMap<>(mmProps));
            }

            perNodeProps.get(2).put("mm.rest.server.legacy.mode", "false"); // New rest server dedicated mode

            final SourceAndTarget sourceAndTarget = new SourceAndTarget(a, b);
            // Bring up a three-node cluster

            for (int i = 0; i < numNodes; i++) {
                startMirrorMaker("node " + i, perNodeProps.get(i));
            }

            // wait for mirror maker to start
            for (int i = 0; i < numNodes; i++) {

                awaitMirrorMakerStart(mirrorMakers.get("node " + i), sourceAndTarget);
                String restUrl = ((DistributedHerder) mirrorMakers.get("node " + i).getStartedHerders().get(sourceAndTarget)).getRestUrl();
                try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
                    String connectorsUrl = createUriBuilderFromLeaderUrl(restUrl, sourceAndTarget).path("connectors").build().toString();
                    HttpResponse response = client.execute(new HttpGet(connectorsUrl));
                    // Validate that the response is successful, and the rest server is available
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity, "UTF-8");
                    if (response.getStatusLine().getStatusCode() == 404) { //The new REST server is not exposing the connectors endpoint
                        assertEquals("{\"error_code\":404,\"message\":\"HTTP 404 Not Found\"}", responseString);
                        // If the rest server is not available, but the response contains the expected 404
                        // json message from the rest server, than the Rest server is up and running
                    } else {
                        assertEquals(200, response.getStatusLine().getStatusCode());
                    }
                }
            }

            String restUrl = ((DistributedHerder) mirrorMakers.get("node 2").getStartedHerders().get(sourceAndTarget)).getRestUrl();
            assertTrue(restUrl.endsWith(MirrorMaker.NEW_REST_CLIENT_MARKER_SUFFIX));


            // wait for heartbeat connector to start running
            awaitConnectorTasksStart(mirrorMakers.get("node 0"), MirrorHeartbeatConnector.class, sourceAndTarget);

            final int messagesPerTopic = 10;
            // Create one topic per Kafka cluster per MirrorMaker node
            for (int i = 0; i < numNodes; i++) {
                String topic = testTopicPrefix + i;

                // Create the topic on cluster A
                clusterA.createTopic(topic, 1);
                // and wait for MirrorMaker to create it on cluster B
                awaitTopicCreation(b, adminB, a + "." + topic);

                // wait for source connector to start running
                awaitConnectorTasksStart(mirrorMakers.get("node " + i), MirrorSourceConnector.class, sourceAndTarget);

                // Write data to the topic on cluster A
                writeToTopic(clusterA, topic, messagesPerTopic);
                // and wait for MirrorMaker to copy it to cluster B
                awaitTopicContent(clusterB, b, a + "." + topic, messagesPerTopic);
            }

            // Perform a rolling restart of the cluster with a new configuration
            String newConfigValue = "2";

            for (int i = 0; i < numNodes; i++) {
                perNodeProps.get(i).put(MirrorSourceConfig.REFRESH_TOPICS_INTERVAL_SECONDS, newConfigValue);
            }

            for (int i = 0; i < numNodes; i++) {
                stopMirrorMaker("node " + i);
                MirrorMaker any = mirrorMakers.values().stream().findAny().get();
                // Wait for the cluster finish the reassignment and rebalance before bringing up the next node.
                awaitConnectorTasksStart(any, MirrorHeartbeatConnector.class, sourceAndTarget);
                awaitConnectorTasksStart(any, MirrorSourceConnector.class, sourceAndTarget);
                startMirrorMaker("node " + i, perNodeProps.get(i));
                awaitMirrorMakerStart(mirrorMakers.get("node " + i), sourceAndTarget);
            }
            // Assert that the new configuration is propagated
            awaitTaskConfigurations(mirrorMakers.get("node 0"), MirrorSourceConnector.class, sourceAndTarget,
                    config -> newConfigValue.equals(config.get(MirrorSourceConfig.REFRESH_TOPICS_INTERVAL_SECONDS)));
        }
    }

    private UriBuilder createUriBuilderFromLeaderUrl(String leaderUrl, SourceAndTarget sourceAndTarget) {
        if (leaderUrl.endsWith(MirrorMaker.NEW_REST_CLIENT_MARKER_SUFFIX)) {
            String cleanLeaderUrl = leaderUrl.replace(MirrorMaker.NEW_REST_CLIENT_MARKER_SUFFIX, "");
            return namespacedUrl(cleanLeaderUrl, sourceAndTarget);
        } else {
            return UriBuilder.fromUri(leaderUrl);
        }
    }

    protected UriBuilder namespacedUrl(String workerUrl, SourceAndTarget sourceAndTarget) {
        UriBuilder result = UriBuilder.fromUri(workerUrl);
        for (String namespacePath : Arrays.asList(sourceAndTarget.source(), sourceAndTarget.target())) {
            result = result.path(namespacePath);
        }
        return result;
    }

    private void awaitTopicCreation(String clusterName, Admin admin, String topic) throws Exception {
        waitForCondition(
                () -> {
                    try {
                        Set<String> allTopics = admin.listTopics().names().get();
                        return allTopics.contains(topic);
                    } catch (Exception e) {
                        log.debug("Failed to check for existence of topic {} on cluster {}", topic, clusterName, e);
                        return false;
                    }
                },
                TOPIC_CREATION_TIMEOUT_MS,
                "topic " + topic + " was not created on cluster " + clusterName + " in time"
        );
    }

    private void writeToTopic(EmbeddedKafkaCluster cluster, String topic, int numMessages) {
        for (int i = 0; i <= numMessages; i++) {
            cluster.produce(topic, Integer.toString(i));
        }
    }
    private void awaitMirrorMakerStart(final MirrorMaker mm, final SourceAndTarget sourceAndTarget) throws InterruptedException {
        awaitMirrorMakerStart(mm, sourceAndTarget, CONNECTOR_CLASSES);
    }

    private void awaitMirrorMakerStart(final MirrorMaker mm, final SourceAndTarget sourceAndTarget, List<Class<?>> connectorClasses) throws InterruptedException {
        waitForCondition(() -> {
            try {
                return connectorClasses.stream().allMatch(
                    connectorClazz -> isConnectorRunningForMirrorMaker(connectorClazz, mm, sourceAndTarget));
            } catch (Exception ex) {
                log.error("Something unexpected occurred. Unable to check for startup status for mirror maker {}", mm, ex);
                throw new NoRetryException(ex);
            }
        }, MM_START_UP_TIMEOUT_MS, "MirrorMaker instances did not transition to running in time");
    }

    private <T extends SourceConnector> void awaitConnectorTasksStart(final MirrorMaker mm, final Class<T> clazz, final SourceAndTarget sourceAndTarget) throws InterruptedException {
        waitForCondition(() -> {
            try {
                return isTaskRunningForMirrorMakerConnector(clazz, mm, sourceAndTarget);
            } catch (Exception ex) {
                log.error("Something unexpected occurred. Unable to check for startup status of connector {} for mirror maker with source->target={}", clazz.getSimpleName(), sourceAndTarget, ex);
                throw new NoRetryException(ex);
            }
        }, MM_START_UP_TIMEOUT_MS, "Tasks for connector " + clazz.getSimpleName() + " for MirrorMaker instances did not transition to running in time");
    }

    private <T extends SourceConnector> void awaitTaskConfigurations(MirrorMaker mm, Class<T> clazz, SourceAndTarget sourceAndTarget, Predicate<Map<String, String>> predicate) throws InterruptedException {
        String connName = clazz.getSimpleName();
        waitForCondition(() -> {
            try {
                FutureCallback<List<TaskInfo>> cb = new FutureCallback<>();
                mm.taskConfigs(sourceAndTarget, connName, cb);
                return cb.get(MM_START_UP_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .stream()
                        .map(TaskInfo::config)
                        .allMatch(predicate);
            } catch (ExecutionException ex) {
                if (ex.getCause() instanceof RebalanceNeededException) {
                    log.warn("Rebalance needed for connector {} for mirror maker with source->target={}", connName, sourceAndTarget, ex);
                    return false;
                } else {
                    log.error("Something unexpected occurred. Unable to get configuration of connector {} for mirror maker with source->target={}", connName, sourceAndTarget, ex);
                    throw new NoRetryException(ex);
                }
            }
        }, MM_START_UP_TIMEOUT_MS, "Connector configuration for " + connName + " for MirrorMaker instances is incorrect");
    }

    private void awaitTopicContent(EmbeddedKafkaCluster cluster, String clusterName, String topic, int numMessages) throws Exception {
        try (Consumer<?, ?> consumer = cluster.createConsumer(Collections.singletonMap(AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            consumer.subscribe(Collections.singleton(topic));
            AtomicInteger messagesRead = new AtomicInteger(0);
            waitForCondition(
                    () -> {
                        ConsumerRecords<?, ?> records = consumer.poll(Duration.ofSeconds(1));
                        return messagesRead.addAndGet(records.count()) >= numMessages;
                    },
                    TOPIC_REPLICATION_TIMEOUT_MS,
                    () -> "could not read " + numMessages + " from topic " + topic + " on cluster " + clusterName + " in time; only read " + messagesRead.get()
            );
        }
    }

    /**
     * Validates that the underlying connector are running for the given MirrorMaker.
     */
    private boolean isConnectorRunningForMirrorMaker(final Class<?> connectorClazz, final MirrorMaker mm, final SourceAndTarget sourceAndTarget) {
        final String connName = connectorClazz.getSimpleName();
        try {
            final ConnectorStateInfo connectorStatus = mm.connectorStatus(sourceAndTarget, connName);
            if (connectorStatus.connector().state().equals(AbstractStatus.State.FAILED.toString())) {
                throw new NoRetryException(new AssertionError(
                    String.format("Connector %s is in FAILED state for MirrorMaker %s and source->target=%s",
                            connectorClazz, mm, sourceAndTarget)));
            }
            // verify that connector state is set to running
            return connectorStatus.connector().state().equals(AbstractStatus.State.RUNNING.toString());
        } catch (NotFoundException nf) {
            // Expected exception thrown by connectorStatus() when connect is not registered
            return false;
        }
    }

    /**
     * Validates that the tasks are associated with the connector and they are running for the given MirrorMaker.
     */
    private <T extends SourceConnector> boolean isTaskRunningForMirrorMakerConnector(final Class<T> connectorClazz, final MirrorMaker mm, final SourceAndTarget sourceAndTarget) {
        final String connName = connectorClazz.getSimpleName();
        final ConnectorStateInfo connectorStatus = mm.connectorStatus(sourceAndTarget, connName);
        return isConnectorRunningForMirrorMaker(connectorClazz, mm, sourceAndTarget)
            // verify that at least one task exists
            && !connectorStatus.tasks().isEmpty()
            // verify that tasks are set to running
            && connectorStatus.tasks().stream().allMatch(s -> {
                if (s.state().equals(AbstractStatus.State.FAILED.toString()))
                    throw new NoRetryException(new AssertionError(String.format("Task %s is in FAILED state", s)));
                return s.state().equals(AbstractStatus.State.RUNNING.toString());
            });
    }
}
