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
package com.cloudera.kafka.connect.secret.store;

import kafka.api.IntegrationTestHarness;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import com.cloudera.kafka.connect.secret.IdentityCipher;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.nio.charset.StandardCharsets;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaSecretStorageIntegrationTest extends IntegrationTestHarness {
    public static final String CONNECTOR = "test_connector";
    private static final String CIPHER_ID_1 = "test_cipher1";
    private static final String CIPHER_ID_2 = "test_cipher2";
    private static final String CIPHER_ID_3 = "test_cipher3";
    private static final String CIPHER_ID_4 = "test_cipher4";
    private static final long OPERATION_TIMEOUT = 5000;
    private static final long DELETE_DELAY = 5000;
    private static final long CONSUMER_POLL_DURATION = 1000;

    private Admin admin;
    private String topic;
    private TopicPartition topicPartition;
    private KafkaSecretStorage kafkaSecretStorage1;
    private KafkaSecretStorage kafkaSecretStorage2;

    @Override
    public int brokerCount() {
        return 1;
    }

    @BeforeEach
    public void setup(TestInfo testInfo) {
        admin = createAdminClient(listenerName(), new Properties());

        //noinspection OptionalGetWithoutIsPresent
        topic = testInfo.getTestMethod().get().getName();
        topicPartition = new TopicPartition(topic, 0);

        kafkaSecretStorage1 = createStorage(CIPHER_ID_1, null);
        kafkaSecretStorage2 = createStorage(CIPHER_ID_2, null);
    }

    @AfterEach
    public void teardown() {
        IdentityCipher.getEncryptionKeys().clear();

        if (kafkaSecretStorage1 != null) {
            kafkaSecretStorage1.close();
        }
        if (kafkaSecretStorage2 != null) {
            kafkaSecretStorage2.close();
        }
        if (admin != null) {
            admin.close();
        }
    }

    @Test
    public void testSingleEncryptionKeyIsSelected() {
        assertEquals(2, IdentityCipher.getEncryptionKeys().size());
        assertArrayEquals(
                IdentityCipher.getEncryptionKeys().get(CIPHER_ID_1),
                IdentityCipher.getEncryptionKeys().get(CIPHER_ID_2),
                "The 2 encryption keys must match"
        );

        try (KafkaSecretStorage ignored = createStorage(CIPHER_ID_3, null)) {
            assertArrayEquals(
                    IdentityCipher.getEncryptionKeys().get(CIPHER_ID_1),
                    IdentityCipher.getEncryptionKeys().get(CIPHER_ID_3),
                    "The 3rd encryption key must match the previous 2"
            );
        }
    }

    @Test
    public void testUpdatedEncryptionKeyIsPickedUp() {
        byte[] updatedKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (KafkaSecretStorage ignored = createStorage(CIPHER_ID_3, updatedKey)) {
            assertArrayEquals(
                    IdentityCipher.getEncryptionKeys().get(CIPHER_ID_1),
                    IdentityCipher.getEncryptionKeys().get(CIPHER_ID_3),
                    "The 3rd encryption key must match the previous 2"
            );
        }
        try (KafkaSecretStorage ignored = createStorage(CIPHER_ID_4, updatedKey)) {
            assertArrayEquals(
                    updatedKey,
                    IdentityCipher.getEncryptionKeys().get(CIPHER_ID_4),
                    "The 4th encryption key must match the updated value"
            );
        }
    }

    @Test
    public void testSavingAndReading() {
        Map<String, String> secrets = baseSecrets();

        String id = kafkaSecretStorage1.saveSecrets(CONNECTOR, secrets);

        Map<String, String> actualSecrets = kafkaSecretStorage2.getSecrets(CONNECTOR, id, false);
        assertEquals(secrets, actualSecrets);
    }

    @Test
    public void testDelete() {
        Map<String, String> secrets = baseSecrets();

        String id = kafkaSecretStorage1.saveSecrets(CONNECTOR, secrets);

        kafkaSecretStorage2.deleteSecrets(CONNECTOR, id);

        // Due to the delay, we expect the version to be available
        assertNotNull(kafkaSecretStorage2.getSecrets(CONNECTOR, id, false));
        waitForSecretToDisappear(kafkaSecretStorage2, id);
        waitForSecretToDisappear(kafkaSecretStorage1, id);
    }

    @Test
    public void testConcurrentModificationExceptionThrownOnUnfinished() {
        Map<String, String> secrets = baseSecrets();

        kafkaSecretStorage1.saveSecrets(CONNECTOR, secrets);

        secrets.put("secret3", "value3");
        assertThrows(ConcurrentModificationException.class, () -> kafkaSecretStorage2.saveSecrets(CONNECTOR, secrets));
    }

    @Test
    public void testConcurrentModificationExceptionNotThrownOnMarkedForCompleted() {
        Map<String, String> secrets = baseSecrets();

        String id = kafkaSecretStorage1.saveSecrets(CONNECTOR, secrets);

        // wait for completion record to get into topic
        awaitEndOffsetToIncrease(() -> kafkaSecretStorage1.markForCompletionAndDeletePrevious(CONNECTOR, id));

        secrets.put("secret3", "value3");
        kafkaSecretStorage2.saveSecrets(CONNECTOR, secrets);
    }

    @Test
    public void testConcurrentModificationExceptionNotThrownOnDeleted() {
        Map<String, String> secrets = baseSecrets();

        String id = kafkaSecretStorage1.saveSecrets(CONNECTOR, secrets);

        // wait for delete record to get into topic
        awaitEndOffsetToIncrease(() -> kafkaSecretStorage1.deleteSecrets(CONNECTOR, id));

        secrets.put("secret3", "value3");
        kafkaSecretStorage2.saveSecrets(CONNECTOR, secrets);
    }

    @Test
    public void testCleanupOfPreviousVersions() {
        Map<String, String> secrets = baseSecrets();

        String id = kafkaSecretStorage1.saveSecrets(CONNECTOR, secrets);

        // wait for completion record to get into topic
        awaitEndOffsetToIncrease(() -> kafkaSecretStorage1.markForCompletionAndDeletePrevious(CONNECTOR, id));

        secrets.put("secret3", "value3");
        String newId = kafkaSecretStorage2.saveSecrets(CONNECTOR, secrets);
        kafkaSecretStorage2.markForCompletionAndDeletePrevious(CONNECTOR, newId);

        waitForSecretToDisappear(kafkaSecretStorage1, id);
        waitForSecretToDisappear(kafkaSecretStorage2, id);
    }

    @Test
    public void testDifferentConnectorsDoNotCauseConcurrentModificationException() {
        String connector1 = "test_connector1";
        String connector2 = "test_connector2";
        Map<String, String> secrets = baseSecrets();

        String id1 = kafkaSecretStorage1.saveSecrets(connector1, secrets);

        String id2 = kafkaSecretStorage1.saveSecrets(connector2, secrets);

        assertEquals(secrets, kafkaSecretStorage2.getSecrets(connector1, id1, false));
        assertEquals(secrets, kafkaSecretStorage2.getSecrets(connector2, id2, false));
    }

    private KafkaSecretStorage createStorage(String cipherId, byte[] updatedKey) {
        Map<String, Object> properties = new HashMap<>();

        properties.put(IdentityCipher.CIPHER_ID, cipherId);
        if (updatedKey != null) {
            properties.put(IdentityCipher.UPDATED_ENCRYPTION_KEY, updatedKey);
        }
        properties.put(KafkaSecretStorageConfig.SECRET_STORAGE_TOPIC_CONFIG, topic);
        properties.put(KafkaSecretStorageConfig.SECRET_CIPHER_CLASS_NAME_CONFIG, IdentityCipher.class.getName());
        properties.put(KafkaSecretStorageConfig.SECRET_DELETE_DELAY_MS_CONFIG, String.valueOf(DELETE_DELAY));
        properties.put(KafkaSecretStorageConfig.SECRET_CONSUMER_POLL_DURATION_MS_CONFIG, String.valueOf(CONSUMER_POLL_DURATION));
        properties.put(KafkaSecretStorageConfig.SECRET_OPERATION_TIMEOUT_MS_CONFIG, String.valueOf(OPERATION_TIMEOUT));
        adminClientConfig().forEach((k, v) -> properties.put((String) k, v));
        KafkaSecretStorage secretStorage = new KafkaSecretStorage();
        secretStorage.configure(properties);
        return secretStorage;
    }

    private void waitForSecretToDisappear(KafkaSecretStorage storage, String id) {
        Awaitility
                .await("Waiting for secret to disappear")
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .timeout(2 * CONSUMER_POLL_DURATION + 3 * DELETE_DELAY, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertNull(storage.getSecrets(CONNECTOR, id, false)));
    }

    private void awaitEndOffsetToIncrease(Runnable action) {
        long offset = fetchEndOffset();
        action.run();
        Awaitility
                .await("Waiting for end offset to pass " + offset)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .timeout(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertTrue(offset < fetchEndOffset()));
    }

    private long fetchEndOffset() {
        Map<TopicPartition, OffsetSpec> spec = new HashMap<>();
        spec.put(topicPartition, OffsetSpec.latest());
        try {
            return admin.listOffsets(spec).all().get().get(topicPartition).offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> baseSecrets() {
        Map<String, String> secrets = new HashMap<>();
        secrets.put("secret1", "value1");
        secrets.put("secret2", "value2");
        return secrets;
    }
}
