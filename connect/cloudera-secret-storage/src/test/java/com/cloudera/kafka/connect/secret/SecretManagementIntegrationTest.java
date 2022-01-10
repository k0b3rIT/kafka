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
// Copyright (c) 2022 Cloudera, Inc. All rights reserved.
package com.cloudera.kafka.connect.secret;

import kafka.utils.TestUtils;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.WorkerHandle;

import com.cloudera.kafka.connect.secret.cipher.SecretCipherConfig;
import com.cloudera.kafka.connect.secret.store.KafkaSecretStorage;
import com.cloudera.kafka.connect.secret.store.KafkaSecretStorageConfig;
import com.cloudera.kafka.connect.secret.store.SecretBundle;
import com.cloudera.kafka.connect.secret.store.SecretBundleMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SecretManagementIntegrationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String CONFIG_PROVIDER_ALIAS = "secret";
    private static final String SECRET_STORAGE_TOPIC_NAME = "test_secret_store";

    private static EmbeddedConnectCluster connect;
    private static KafkaSecretStorage secretStorage;

    @TempDir
    static Path cipherTempDir;

    @TempDir
    Path tempDir;
    private String testName;

    @BeforeAll
    public static void setup() {
        Properties brokerProps = new Properties();
        brokerProps.put("listeners", "PLAINTEXT://localhost:9092");

        Map<String, String> workerProps = new HashMap<>();
        //Register extension
        workerProps.put(RestServerConfig.REST_EXTENSION_CLASSES_CONFIG, SecretStorageCachingExtension.class.getName());
        //Enable secret storage
        workerProps.put(ConnectSecretExtensionConfig.STORAGE_ENABLED_CONFIG, "true");
        workerProps.put("config.providers.secret.param.bootstrap.servers", "localhost:9092");
        //Configure Kafka secret storage
        workerProps.put("config.providers.secret.param." + KafkaSecretStorageConfig.SECRET_STORAGE_TOPIC_CONFIG, SECRET_STORAGE_TOPIC_NAME);
        workerProps.putAll(cipherConfigs("config.providers.secret.param."));
        //Configure secret provider
        workerProps.put(DistributedConfig.CONFIG_PROVIDERS_CONFIG, CONFIG_PROVIDER_ALIAS);
        workerProps.put("config.providers.secret.class", CachedSecretStorageFetcherConfigProvider.class.getName());

        connect = new EmbeddedConnectCluster.Builder()
                .name("secret-connect-cluster")
                .numWorkers(2)
                .numBrokers(1)
                .workerProps(workerProps)
                .brokerProps(brokerProps)
                .build();
        connect.start();

        Awaitility
                .await("Wait for secret storage topic to appear")
                .timeout(2, TimeUnit.MINUTES)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    Map<String, Optional<TopicDescription>> topics = connect.kafka()
                            .describeTopics(SECRET_STORAGE_TOPIC_NAME);
                    Optional<TopicDescription> topicDescription = topics.get(SECRET_STORAGE_TOPIC_NAME);
                    assertNotNull(topicDescription);
                    assertTrue(topicDescription.isPresent());
                });
        secretStorage = createSecretStorage();
    }

    @AfterAll
    public static void teardown() {
        if (secretStorage != null) {
            secretStorage.close();
        }
        connect.stop();
        CachedSecretStorageFetcherConfigProvider.closeSecretStorages();
        CachedSecretStorageFetcherConfigProvider.reset();
        TestUtils.verifyNoUnexpectedThreads("SecretManagementIntegrationTest.teardown");
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        //noinspection OptionalGetWithoutIsPresent
        testName = testInfo.getTestMethod().get().getName();
    }

    @AfterEach
    public void afterEach() {
    }

    @Test
    public void testSubmitWithSecretPathToDifferentWorkers() {
        int i = 0;
        for (WorkerHandle workerHandle : connect.workers()) {
            String connectorName = testName + "FileConnector" + i;
            String topicName = testName + "_source_topic" + i;
            Path connectorOutputFile = getTempFilePath(connectorName + ".txt");

            //Produce data into source topic
            List<String> messages = createTopicAndProduceUniqueMessages(topicName);

            Map<String, String> connectorConfig = createFileSinkConnectorConfig(topicName, connectorOutputFile);

            ConnectorInfo connectorInfo = createFileSinkConnector(workerHandle, connectorName, connectorConfig);
            String bundleId = assertConfigWithBundleId(connectorInfo, connectorConfig);

            assertMessagesAppeared("for connector " + connectorName, messages, connectorOutputFile);

            Map<String, String> secrets = secretMap(connectorOutputFile);
            assertSecretsInStorage(connectorName, bundleId, secrets);
            assertSecretMarkedCompleted(connectorName, bundleId);

            ++i;
        }
    }

    @Test
    public void testSecretEdit() {
        String connectorName = testName + "FileConnector";
        String topicName = testName + "_source_topic";
        Path connectorOutputFile1 = getTempFilePath(connectorName + "1.txt");
        Path connectorOutputFile2 = getTempFilePath(connectorName + "2.txt");

        //Produce data into source topic
        List<String> messages1 = createTopicAndProduceUniqueMessages(topicName);

        Map<String, String> connectorConfig1 = createFileSinkConnectorConfig(topicName, connectorOutputFile1);

        ConnectorInfo connectorInfo = createFileSinkConnector(connectorName, connectorConfig1);
        String bundleId1 = assertConfigWithBundleId(connectorInfo, connectorConfig1);

        assertMessagesAppeared("for first config version", messages1, connectorOutputFile1);

        Map<String, String> secrets1 = secretMap(connectorOutputFile1);
        assertSecretsInStorage(connectorName, bundleId1, secrets1);
        assertSecretMarkedCompleted(connectorName, bundleId1);

        Map<String, String> connectorConfig2 = new HashMap<>(connectorConfig1);
        //Changing existing secret
        connectorConfig2.put(FileStreamSinkConnector.FILE_CONFIG, connectorOutputFile2.toAbsolutePath().toString());
        connectorInfo = parseInfo(connect.configureConnector(connectorName, connectorConfig2));
        //We don't expect the config to be changed
        String bundleId2 = assertConfigWithBundleId(connectorInfo, connectorConfig2);
        //But the bundle id should be changed
        assertNotEquals(bundleId1, bundleId2);

        assertFileExists(connectorOutputFile2);

        //Produce more data into source topic
        List<String> messages2 = produceUniqueMessages(topicName);

        assertMessagesAppeared("for second config version", messages2, connectorOutputFile2);

        Map<String, String> secrets2 = secretMap(connectorOutputFile2);
        assertSecretsInStorage(connectorName, bundleId2, secrets2);
        assertSecretMarkedCompleted(connectorName, bundleId2);
        assertSecretMarkedDeleted(connectorName, bundleId1);
    }

    @Test
    public void testSecretDelete() {
        String connectorName = testName + "FileConnector";
        String topicName = testName + "_source_topic";
        Path connectorOutputFile = getTempFilePath(connectorName + ".txt");

        //Produce data into source topic
        List<String> messages = createTopicAndProduceUniqueMessages(topicName);

        Map<String, String> connectorConfig = createFileSinkConnectorConfig(topicName, connectorOutputFile);

        ConnectorInfo connectorInfo = createFileSinkConnector(connectorName, connectorConfig);
        String bundleId = assertConfigWithBundleId(connectorInfo, connectorConfig);

        assertMessagesAppeared("for connector " + connectorName, messages, connectorOutputFile);

        Map<String, String> secrets = secretMap(connectorOutputFile);
        assertSecretsInStorage(connectorName, bundleId, secrets);
        assertSecretMarkedCompleted(connectorName, bundleId);

        connect.deleteConnector(connectorName);

        assertSecretMarkedDeleted(connectorName, bundleId);
    }

    @Test
    public void testValidationOk() {
        String connectorName = testName + "FileConnector";
        String topicName = testName + "_source_topic";
        Path connectorOutputFile = getTempFilePath(connectorName + ".txt");
        Map<String, String> connectorConfig = createFileSinkConnectorConfig(topicName, connectorOutputFile);
        connectorConfig.put("name", connectorName);

        ConfigInfos infos = connect.validateConnectorConfig(FileStreamSinkConnector.class.getName(), connectorConfig);

        assertEquals(0, infos.errorCount());
    }

    @Test
    public void testValidationFailsWhenBundleDoesNotExist() {
        String connectorName = testName + "FileConnector";
        String topicName = testName + "_source_topic";
        Path connectorOutputFile = getTempFilePath(connectorName + ".txt");
        Map<String, String> connectorConfig = createFileSinkConnectorConfig(topicName, connectorOutputFile);
        connectorConfig.put("name", connectorName);
        connectorConfig.put(SecretStorageExtension.SECRET_BUNDLE_ID, "invalid");

        ConfigInfos infos = connect.validateConnectorConfig(FileStreamSinkConnector.class.getName(), connectorConfig);

        assertEquals(1, infos.errorCount());
    }

    @Test
    public void testValidationFailsWhenSecretReferenceIsPresentWithoutBundleId() {
        String connectorName = testName + "FileConnector";
        String topicName = testName + "_source_topic";
        String extraPropName = "some_extra_secret_prop";
        Path connectorOutputFile = getTempFilePath(connectorName + ".txt");
        Map<String, String> connectorConfig = createFileSinkConnectorConfig(topicName, connectorOutputFile);
        connectorConfig.put("name", connectorName);
        connectorConfig.put(extraPropName, "${secret:" + connectorName + ":" + extraPropName + "}");
        connectorConfig.put(SecretStorageExtension.SENSITIVE_PROPERTY_LIST, extraPropName);

        ConfigInfos infos = connect.validateConnectorConfig(FileStreamSinkConnector.class.getName(), connectorConfig);

        assertEquals(1, infos.errorCount());
    }

    private static Map<String, String> secretMap(Path file) {
        Map<String, String> secrets = new HashMap<>();
        secrets.put(FileStreamSinkConnector.FILE_CONFIG, file.toAbsolutePath().toString());
        return secrets;
    }

    private static void assertSecretsInStorage(String connector, String id, Map<String, String> secrets) {
        Awaitility
                .await("Wait for secret bundle: " + connector + " " + id)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .timeout(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    Map<String, String> actualSecrets = secretStorage.getSecrets(connector, id, false);
                    assertEquals(secrets, actualSecrets);
                });
    }

    private static void assertSecretMarkedCompleted(String connector, String id) {
        Awaitility
                .await("Wait for secret bundle to be marked deleted: " + connector + " " + id)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .timeout(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    SecretBundleMap<Map<String, String>> allSecrets = secretStorage.getConnectorSecrets();
                    SecretBundle<Map<String, String>> bundle = allSecrets.getBundle(connector, id);
                    assertNotNull(bundle);
                    assertTrue(bundle.isCompleted());
                });
    }

    private static void assertSecretMarkedDeleted(String connector, String id) {
        Awaitility
                .await("Wait for secret bundle to be marked deleted: " + connector + " " + id)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .timeout(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    SecretBundleMap<Map<String, String>> allSecrets = secretStorage.getConnectorSecrets();
                    SecretBundle<Map<String, String>> bundle = allSecrets.getBundle(connector, id);
                    assertNotNull(bundle);
                    assertTrue(bundle.isDeleted());
                });
    }

    private static String assertConfigWithBundleId(ConnectorInfo connectorInfo, Map<String, String> expectedConfig) {
        Map<String, String> actualConnectorConfig = new HashMap<>(connectorInfo.config());
        String bundleId = actualConnectorConfig.remove(SecretStorageExtension.SECRET_BUNDLE_ID);
        assertNotNull(bundleId);
        Map<String, String> expectedConfigs = new HashMap<>(expectedConfig);
        expectedConfigs.put(ConnectorConfig.NAME_CONFIG, connectorInfo.name());
        expectedConfigs.put(
                FileStreamSinkConnector.FILE_CONFIG,
                "${secret:" + connectorInfo.name() + "/" + bundleId + ":" + FileStreamSinkConnector.FILE_CONFIG + "}"
        );
        assertEquals(expectedConfigs, actualConnectorConfig);
        return bundleId;
    }

    private static KafkaSecretStorage createSecretStorage() {
        String bootstrapServers = connect.kafka().bootstrapServers();
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put(KafkaSecretStorageConfig.SECRET_STORAGE_TOPIC_CONFIG, SECRET_STORAGE_TOPIC_NAME);
        props.putAll(cipherConfigs(""));
        KafkaSecretStorage secretStorage = new KafkaSecretStorage();
        secretStorage.configure(props);
        return secretStorage;
    }

    private static Map<String, String> cipherConfigs(String prefix) {
        Map<String, String> props = new HashMap<>();
        props.put(prefix + SecretCipherConfig.GLOBAL_KEY_LOCATION_CONFIG, cipherTempDir.toAbsolutePath().toString());
        props.put(prefix + SecretCipherConfig.PBE_SALT_CONFIG, "Some seasoning with enough entropy.");
        props.put(prefix + SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, "SuperSecret!");
        return props;
    }

    private static List<String> createTopicAndProduceUniqueMessages(String topicName) {
        connect.kafka().createTopic(topicName);
        return produceUniqueMessages(topicName);
    }

    private static List<String> produceUniqueMessages(String topicName) {
        List<String> messages = IntStream.range(0, 10)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(Collectors.toList());
        messages.forEach(msg -> connect.kafka().produce(topicName, msg));
        return messages;
    }

    private static void assertMessagesAppeared(String label, List<String> messages, Path file) {
        assertFileExists(file);
        Awaitility
                .await("Wait for records to appear in file " + label)
                .timeout(5, TimeUnit.MINUTES)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(messages, readTempFileLines(file)));
    }

    private static Map<String, String> createFileSinkConnectorConfig(String topic, Path outputFile) {
        Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSinkConnector.class.getName());
        connectorConfig.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
        connectorConfig.put(SecretStorageExtension.SENSITIVE_PROPERTY_LIST, FileStreamSinkConnector.FILE_CONFIG);
        connectorConfig.put(FileStreamSinkConnector.FILE_CONFIG, outputFile.toAbsolutePath().toString());
        return connectorConfig;
    }

    private static ConnectorInfo createFileSinkConnector(String connectorName, Map<String, String> connectorConfig) {
        return createFileSinkConnector(connect.workers().iterator().next(), connectorName, connectorConfig);
    }

    private static ConnectorInfo createFileSinkConnector(WorkerHandle workerHandle, String connectorName,
                                                         Map<String, String> connectorConfig) {
        URI workerUrl = workerHandle.url();
        String url = workerUrl.toString() + "connectors/" + connectorName + "/config";
        String body = toJson(connectorConfig);
        String config;
        try (Response response = connect.requestPut(url, body)) {
            assertEquals(201, response.getStatus(), (String) response.getEntity());
            config = (String) response.getEntity();
        }
        return parseInfo(config);
    }

    private static String toJson(Object o) {
        try {
            return OBJECT_MAPPER.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectorInfo parseInfo(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, ConnectorInfo.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> readTempFileLines(Path tempFile) {
        try {
            return Files.readAllLines(tempFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertFileExists(Path file) {
        Awaitility
                .await("Wait for file to appear: " + file.toFile().getName())
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .timeout(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertTrue(file.toFile().exists()));
    }

    private Path getTempFilePath(String filename) {
        return Paths.get(tempDir.toString(), filename);
    }
}
