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
package com.cloudera.kafka.connect.secret.tool;

import kafka.api.IntegrationTestHarness;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.cloudera.kafka.connect.secret.cipher.DefaultSecretCipher;
import com.cloudera.kafka.connect.secret.cipher.SecretCipherConfig;
import com.cloudera.kafka.connect.secret.store.CompletionMarkKey;
import com.cloudera.kafka.connect.secret.store.EncryptionKey;
import com.cloudera.kafka.connect.secret.store.KafkaSecretStorage;
import com.cloudera.kafka.connect.secret.store.KafkaSecretStorageConfig;
import com.cloudera.kafka.connect.secret.store.RecordKey;
import com.cloudera.kafka.connect.secret.store.SecretBundle;
import com.cloudera.kafka.connect.secret.store.SecretBundleKey;
import com.cloudera.kafka.connect.secret.store.SecretBundleMap;
import com.cloudera.kafka.connect.secret.store.StorageKeyDeserializer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReEncryptionToolIntegrationTest extends IntegrationTestHarness {
    private static final String OLD_STORAGE_TOPIC_NAME = "old_secrets";
    private static final String NEW_STORAGE_TOPIC_NAME = "new_secrets";
    private static final String CONNECTOR_SINGLE_BUNDLE = "connector_single_bundle";
    private static final String CONNECTOR_MULTIPLE_BUNDLES = "connector_multiple_bundles";
    private static final String CONNECTOR_OLD_BUNDLE_CLEANUP = "connector_old_bundle_cleanup";
    private static final String CONNECTOR_DELETED_BUNDLE = "connector_deleted_bundle";

    private KafkaSecretStorage oldSecretStorage;
    private KafkaSecretStorage newSecretStorage;
    private SecretBundleMap<Map<String, String>> allBundles;

    @Override
    public int brokerCount() {
        return 1;
    }

    @BeforeEach
    public void setup() {
        Map<String, Object> oldConfig = storageConfig(OLD_STORAGE_TOPIC_NAME, oldPasswordConfig());
        oldSecretStorage = new KafkaSecretStorage();
        oldSecretStorage.configure(oldConfig);
    }

    @AfterEach
    public void teardown() {
        if (oldSecretStorage != null) {
            oldSecretStorage.close();
        }
        if (newSecretStorage != null) {
            newSecretStorage.close();
        }
    }

    @Test
    public void testMigration() {
        // Save a connector with a single bundle
        Map<String, String> singleBundleSecrets = baseSecrets();
        String singleBundleId = oldSecretStorage.saveSecrets(CONNECTOR_SINGLE_BUNDLE, singleBundleSecrets);

        // Save a connector with 2 bundles
        Map<String, String> multipleBundleFirstSecrets = baseSecrets();
        String multipleBundleFirstId =
                oldSecretStorage.saveSecrets(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleFirstSecrets);
        oldSecretStorage.markForCompletionAndDeletePrevious(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleFirstId);
        Map<String, String> multipleBundleSecondSecrets = baseSecrets();
        multipleBundleSecondSecrets.put("secret3", "value3");
        String multipleBundleSecondId =
                oldSecretStorage.saveSecrets(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleSecondSecrets);

        // Save a connector with 2 bundles, but clean up the older version
        Map<String, String> oldCleanupFirstSecrets = baseSecrets();
        String oldCleanupFirstId =
                oldSecretStorage.saveSecrets(CONNECTOR_OLD_BUNDLE_CLEANUP, oldCleanupFirstSecrets);
        oldSecretStorage.markForCompletionAndDeletePrevious(CONNECTOR_OLD_BUNDLE_CLEANUP, oldCleanupFirstId);
        Map<String, String> oldCleanupSecondSecrets = baseSecrets();
        oldCleanupSecondSecrets.put("secret3", "value3");
        String oldCleanupSecondId = oldSecretStorage.saveSecrets(CONNECTOR_OLD_BUNDLE_CLEANUP, oldCleanupSecondSecrets);
        oldSecretStorage.markForCompletionAndDeletePrevious(CONNECTOR_OLD_BUNDLE_CLEANUP, oldCleanupSecondId);

        // Save a connector with 1 bundle, then delete it
        String deletedId = oldSecretStorage.saveSecrets(CONNECTOR_DELETED_BUNDLE, baseSecrets());
        oldSecretStorage.deleteSecrets(CONNECTOR_DELETED_BUNDLE, deletedId);

        // Stop old storage, flushes messages
        oldSecretStorage.close();

        Map<String, Object> oldConfig = storageConfig(OLD_STORAGE_TOPIC_NAME, oldPasswordConfig());
        Map<String, Object> newConfig = storageConfig(NEW_STORAGE_TOPIC_NAME, newPasswordConfig());

        // Execute the migration
        ReEncryptionTool tool = new ReEncryptionTool(oldConfig, newConfig);
        tool.run();

        newSecretStorage = new KafkaSecretStorage();
        newSecretStorage.configure(newConfig);

        allBundles = newSecretStorage.getConnectorSecrets();

        // Check single bundle and its contents
        assertSecretBundleContent(CONNECTOR_SINGLE_BUNDLE, singleBundleId, singleBundleSecrets);
        assertNoPreviousSecrets(CONNECTOR_SINGLE_BUNDLE, singleBundleId);

        // Check multiple bundles and their order
        assertSecretBundleContent(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleFirstId, multipleBundleFirstSecrets);
        assertSecretBundleContent(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleSecondId, multipleBundleSecondSecrets);
        assertNoPreviousSecrets(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleFirstId);
        assertPreviousSecrets(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleSecondId,
                Collections.singletonList(multipleBundleFirstId));

        // Check old cleanup bundle
        assertSecretBundleContent(CONNECTOR_OLD_BUNDLE_CLEANUP, oldCleanupSecondId, oldCleanupSecondSecrets);
        assertSecretBundleNotPresent(CONNECTOR_OLD_BUNDLE_CLEANUP, oldCleanupFirstId);
        assertNoPreviousSecrets(CONNECTOR_OLD_BUNDLE_CLEANUP, oldCleanupSecondId);

        // Check deleted bundle
        assertSecretBundleNotPresent(CONNECTOR_DELETED_BUNDLE, deletedId);

        assertEncryptionKeysAreDifferent();

        Map<String, List<RecordKey>> expectedKeyOrderPerConnector = new HashMap<>();

        // Single bundle has a single secret saved
        expectedKeyOrderPerConnector
                .computeIfAbsent(CONNECTOR_SINGLE_BUNDLE, k -> new ArrayList<>())
                .add(new SecretBundleKey(CONNECTOR_SINGLE_BUNDLE, singleBundleId));

        // Multiple bundles should have both bundles, first one is completed
        List<RecordKey> multipleBundleKeys = new ArrayList<>();
        multipleBundleKeys.add(new SecretBundleKey(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleFirstId));
        multipleBundleKeys.add(new CompletionMarkKey(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleFirstId));
        multipleBundleKeys.add(new SecretBundleKey(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleSecondId));
        expectedKeyOrderPerConnector.put(CONNECTOR_MULTIPLE_BUNDLES, multipleBundleKeys);

        // Old cleanup should only appear with the latest bundle, which is also marked completed
        List<RecordKey> cleanupBundleKeys = new ArrayList<>();
        cleanupBundleKeys.add(new SecretBundleKey(CONNECTOR_OLD_BUNDLE_CLEANUP, oldCleanupSecondId));
        cleanupBundleKeys.add(new CompletionMarkKey(CONNECTOR_OLD_BUNDLE_CLEANUP, oldCleanupSecondId));
        expectedKeyOrderPerConnector.put(CONNECTOR_OLD_BUNDLE_CLEANUP, cleanupBundleKeys);

        Map<String, List<RecordKey>> actualKeys = collectSecretAndCompletionKeys(NEW_STORAGE_TOPIC_NAME);
        assertEquals(expectedKeyOrderPerConnector, actualKeys);
    }

    private Map<String, List<RecordKey>> collectSecretAndCompletionKeys(String topic) {
        Map<String, List<RecordKey>> result = new HashMap<>();
        processUntilEnd(topic, record -> {
            String connector = getConnectorFromKey(record.key());
            if (connector == null) {
                return;
            }
            result.computeIfAbsent(connector, k -> new ArrayList<>()).add(record.key());
        });
        return result;
    }

    private String getConnectorFromKey(RecordKey key) {
        if (key instanceof SecretBundleKey) {
            return ((SecretBundleKey) key).getConnector();
        } else if (key instanceof CompletionMarkKey) {
            return ((CompletionMarkKey) key).getConnector();
        }
        return null;
    }

    private void assertEncryptionKeysAreDifferent() {
        byte[] oldEncryptionKey = findLatestEncryptionKey(OLD_STORAGE_TOPIC_NAME);
        byte[] newEncryptionKey = findLatestEncryptionKey(NEW_STORAGE_TOPIC_NAME);
        assertFalse(Arrays.equals(oldEncryptionKey, newEncryptionKey), "The encryption keys must not match");
    }

    private byte[] findLatestEncryptionKey(String topic) {
        AtomicReference<byte[]> lastEncryptionKey = new AtomicReference<>();
        processUntilEnd(topic, record -> {
            if (record.key() instanceof EncryptionKey) {
                lastEncryptionKey.set(record.value());
            }
        });
        return lastEncryptionKey.get();
    }

    private void processUntilEnd(String topic, java.util.function.Consumer<ConsumerRecord<RecordKey, byte[]>> processor) {
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        Map<String, Object> props = new HashMap<>();
        adminClientConfig().forEach((k, v) -> props.put((String) k, v));
        try (Consumer<RecordKey, byte[]> consumer =
                     new KafkaConsumer<>(props, new StorageKeyDeserializer(), new ByteArrayDeserializer())) {
            consumer.assign(Collections.singleton(topicPartition));
            consumer.seekToBeginning(Collections.emptyList());
            long endOffset = consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
            long offset = -1;
            for (ConsumerRecords<RecordKey, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                 offset < endOffset - 1;
                 records = consumer.poll(Duration.ofMillis(1000))) {
                for (ConsumerRecord<RecordKey, byte[]> record : records) {
                    offset = record.offset();
                    processor.accept(record);
                }
            }
        }
    }

    private void assertSecretBundleNotPresent(String connector, String id) {
        assertNull(allBundles.getBundle(connector, id));
    }

    private void assertSecretBundleContent(String connector, String id, Map<String, String> secrets) {
        SecretBundle<Map<String, String>> bundle = allBundles.getBundle(connector, id);
        assertNotNull(bundle);
        assertEquals(secrets, bundle.getSecrets());
    }

    private void assertNoPreviousSecrets(String connector, String id) {
        assertPreviousSecrets(connector, id, Collections.emptyList());
    }

    private void assertPreviousSecrets(String connector, String id, List<String> previousIds) {
        assertEquals(
                previousIds,
                allBundles
                        .findPreviousBundles(connector, id)
                        .stream()
                        .map(SecretBundle::getId)
                        .collect(Collectors.toList())
        );
    }

    private Map<String, String> baseSecrets() {
        Map<String, String> secrets = new HashMap<>();
        secrets.put("secret1", "value1");
        secrets.put("secret2", "value2");
        return secrets;
    }

    private Map<String, Object> storageConfig(String topic, Map<String, Object> pwdConfigs) {
        Map<String, Object> configs = new HashMap<>();
        adminClientConfig().forEach((k, v) -> configs.put((String) k, v));

        configs.put(KafkaSecretStorageConfig.SECRET_STORAGE_TOPIC_CONFIG, topic);
        configs.put(KafkaSecretStorageConfig.SECRET_CIPHER_CLASS_NAME_CONFIG, DefaultSecretCipher.class);
        configs.putAll(pwdConfigs);

        return configs;
    }

    private Map<String, Object> oldPasswordConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SecretCipherConfig.PBE_SALT_CONFIG, "Some seasoning with sufficient entropy.");
        configs.put(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, "SuperSecret!");

        return configs;
    }

    private Map<String, Object> newPasswordConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SecretCipherConfig.PBE_SALT_CONFIG, "Fresh seasoning for exciting new taste.");
        configs.put(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, "DifferentFromTheOldOne!");

        return configs;
    }

}
