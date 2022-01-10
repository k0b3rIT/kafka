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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import com.cloudera.kafka.connect.secret.SecretCipher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class KafkaSecretStorageTest {
    private static final String TOPIC = "secret_storage";
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
    private static final String CONNECTOR = "MyConnector";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SecretCipher mockCipher;
    private Producer<RecordKey, byte[]> mockProducer;
    private ScheduledExecutorService mockDeleteExecutor;
    private KafkaSecretStorage secretStorage;

    @BeforeEach
    public void setup() {
        mockCipher = EasyMock.createMock(SecretCipher.class);
        mockProducer = EasyMock.createMock(Producer.class);
        mockDeleteExecutor = EasyMock.createMock(ScheduledExecutorService.class);
        Consumer<RecordKey, byte[]> mockConsumer = EasyMock.createMock(Consumer.class);
        secretStorage = EasyMock.partialMockBuilder(KafkaSecretStorage.class)
                .withConstructor(
                        mockCipher, TOPIC, new TopicPartition(TOPIC, 0), mockConsumer, mockProducer,
                        10000L, Duration.ZERO, Duration.ZERO, Duration.ZERO, 0L, 0L, mockDeleteExecutor
                )
                .addMockedMethod("waitForConnectorId")
                .createMock();
        expect(secretStorage.waitForConnectorId(anyObject(), anyObject(), anyLong())).andReturn(true).anyTimes();
        replay(secretStorage);
    }

    @Test
    public void testSaveSecrets() {
        Map<String, String> connectorSecrets = new LinkedHashMap<>();
        connectorSecrets.put("key1", "secret1");
        connectorSecrets.put("key2", "secret2");
        setupIdentityCipherEncrypt();

        Capture<ProducerRecord<RecordKey, byte[]>> producerRecordCapture = newCapture(CaptureType.ALL);
        expect(mockProducer.send(capture(producerRecordCapture)))
                .andReturn(CompletableFuture.completedFuture(recordMetadata(1L)));
        mockProducer.flush();
        replay(mockProducer);

        String id = secretStorage.saveSecrets(CONNECTOR, connectorSecrets);

        assertNotNull(id);
        verify(mockCipher, mockProducer);

        List<ProducerRecord<RecordKey, byte[]>> records = producerRecordCapture.getValues();
        assertEquals(1, records.size());
        ProducerRecord<RecordKey, byte[]> insertRecord = records.get(0);
        SecretBundleKey insertKey = (SecretBundleKey) insertRecord.key();
        assertEquals(CONNECTOR, insertKey.getConnector());
        assertDoesNotThrow(() -> UUID.fromString(insertKey.getId()));
        SecretValue secrets = deserializeSecretValue(insertRecord.value());
        assertEquals(id, secrets.getId());
        assertEquals(connectorSecrets, secrets.getSecrets());
    }

    @Test
    public void testDeleteSecrets() {
        SecretBundleKey savedKey = new SecretBundleKey(CONNECTOR, "some_id");
        Map<String, String> connectorSecrets = new LinkedHashMap<>();
        connectorSecrets.put("key1", "secret1");
        connectorSecrets.put("key2", "secret2");

        Capture<ProducerRecord<RecordKey, byte[]>> producerRecordCapture = newCapture(CaptureType.ALL);
        expect(mockProducer.send(capture(producerRecordCapture)))
                .andReturn(CompletableFuture.completedFuture(recordMetadata(10L))); //Delete secrets
        expect(mockProducer.send(capture(producerRecordCapture)))
                .andReturn(CompletableFuture.completedFuture(recordMetadata(11L))); //Delete completion marker
        replay(mockProducer, mockDeleteExecutor);

        setupIdentityCipherDecrypt();
        backfillSecrets(connectorSecrets, savedKey.getId());

        secretStorage.deleteSecrets(CONNECTOR, savedKey.getId());

        verify(mockProducer);
        List<ProducerRecord<RecordKey, byte[]>> records = producerRecordCapture.getValues();
        assertEquals(2, records.size());

        ProducerRecord<RecordKey, byte[]> deleteSecretRecord = records.get(0);
        SecretBundleKey deletedKey = (SecretBundleKey) deleteSecretRecord.key();
        assertEquals(savedKey, deletedKey);
        assertNull(deleteSecretRecord.value());

        ProducerRecord<RecordKey, byte[]> deleteCompletionMarkerRecord = records.get(1);
        CompletionMarkKey deletedMarkerKey = (CompletionMarkKey) deleteCompletionMarkerRecord.key();
        assertEquals(new CompletionMarkKey(CONNECTOR, savedKey.getId()), deletedMarkerKey);
        assertNull(deleteCompletionMarkerRecord.value());
    }

    @Test
    public void testGetSecrets() {
        Map<String, String> connectorSecrets = new LinkedHashMap<>();
        connectorSecrets.put("key1", "secret1");
        connectorSecrets.put("key2", "secret2");

        setupIdentityCipherDecrypt();
        backfillSecrets(connectorSecrets, "some_id");

        assertSecrets(connectorSecrets, "some_id", false);
        verify(mockCipher);
    }

    @Test
    public void testGetSecretsWithDeletedIncluded() {
        Map<String, String> connectorSecrets = new LinkedHashMap<>();
        connectorSecrets.put("key1", "secret1");
        connectorSecrets.put("key2", "secret2");

        setupIdentityCipherDecrypt();
        backfillSecrets(connectorSecrets, "some_id");
        deleteSecret("some_id");

        assertSecrets(connectorSecrets, "some_id", true);
        verify(mockCipher);
    }

    @Test
    public void testGetSecretsWithDeletedExcluded() {
        Map<String, String> connectorSecrets = new LinkedHashMap<>();
        connectorSecrets.put("key1", "secret1");
        connectorSecrets.put("key2", "secret2");

        setupIdentityCipherDecrypt();
        backfillSecrets(connectorSecrets, "some_id");
        deleteSecret("some_id");

        assertSecrets(null, "some_id", false);
        verify(mockCipher);
    }

    @Test
    public void testMarkForCompletion() {
        Map<String, String> connectorSecrets = new LinkedHashMap<>();
        connectorSecrets.put("key1", "secret1");
        connectorSecrets.put("key2", "secret2");
        setupIdentityCipherDecrypt();
        String id1 = "some_id";
        backfillSecrets(connectorSecrets, id1);
        connectorSecrets.put("key3", "secret3");
        String id2 = "another_id";
        backfillSecrets(connectorSecrets, id2);
        connectorSecrets.put("key4", "secret4");
        String id3 = "and_another_id";
        backfillSecrets(connectorSecrets, id3);

        Capture<ProducerRecord<RecordKey, byte[]>> producerRecordCapture = newCapture(CaptureType.ALL);
        expect(mockProducer.send(capture(producerRecordCapture)))
                .andReturn(CompletableFuture.completedFuture(recordMetadata(10L)))
                .anyTimes();
        replay(mockProducer);

        secretStorage.markForCompletionAndDeletePrevious(CONNECTOR, id3);

        List<ProducerRecord<RecordKey, byte[]>> records = producerRecordCapture.getValues();
        assertEquals(5, records.size());

        ProducerRecord<RecordKey, byte[]> markCompletionRecord = records.get(0);
        assertEquals(
                new CompletionMarkKey(CONNECTOR, id3),
                markCompletionRecord.key()
        );
        assertNotNull(markCompletionRecord.value());

        assertDeleteRecord(id1, records.get(1));
        assertDeleteCompletionMarkRecord(id1, records.get(2));

        assertDeleteRecord(id2, records.get(3));
        assertDeleteCompletionMarkRecord(id2, records.get(4));
    }

    @Test
    public void testDeletePreviousSecrets() {
        Map<String, String> connectorSecrets = new LinkedHashMap<>();
        connectorSecrets.put("key1", "secret1");
        connectorSecrets.put("key2", "secret2");
        setupIdentityCipherDecrypt();
        String id1 = "some_id";
        backfillSecrets(connectorSecrets, id1);
        connectorSecrets.put("key3", "secret3");
        String id2 = "another_id";
        backfillSecrets(connectorSecrets, id2);
        connectorSecrets.put("key4", "secret4");
        String id3 = "and_another_id";
        backfillSecrets(connectorSecrets, id3);

        Capture<ProducerRecord<RecordKey, byte[]>> producerRecordCapture = newCapture(CaptureType.ALL);
        expect(mockProducer.send(capture(producerRecordCapture)))
                .andReturn(CompletableFuture.completedFuture(recordMetadata(10L)))
                .anyTimes();
        replay(mockProducer);

        secretStorage.deletePreviousSecrets(CONNECTOR, id3);

        List<ProducerRecord<RecordKey, byte[]>> records = producerRecordCapture.getValues();
        assertEquals(4, records.size());

        assertDeleteRecord(id1, records.get(0));
        assertDeleteCompletionMarkRecord(id1, records.get(1));

        assertDeleteRecord(id2, records.get(2));
        assertDeleteCompletionMarkRecord(id2, records.get(3));
    }

    @Test
    public void testDeleteSecretAndPreviousSecrets() {
        Map<String, String> connectorSecrets = new LinkedHashMap<>();
        connectorSecrets.put("key1", "secret1");
        connectorSecrets.put("key2", "secret2");
        setupIdentityCipherDecrypt();
        String id1 = "some_id";
        backfillSecrets(connectorSecrets, id1);
        connectorSecrets.put("key3", "secret3");
        String id2 = "another_id";
        backfillSecrets(connectorSecrets, id2);
        connectorSecrets.put("key4", "secret4");
        String id3 = "and_another_id";
        backfillSecrets(connectorSecrets, id3);

        Capture<ProducerRecord<RecordKey, byte[]>> producerRecordCapture = newCapture(CaptureType.ALL);
        expect(mockProducer.send(capture(producerRecordCapture)))
                .andReturn(CompletableFuture.completedFuture(recordMetadata(10L)))
                .anyTimes();
        replay(mockProducer);

        secretStorage.deleteSecretsAndPreviousSecrets(CONNECTOR, id3);

        List<ProducerRecord<RecordKey, byte[]>> records = producerRecordCapture.getValues();
        assertEquals(6, records.size());

        assertDeleteRecord(id1, records.get(0));
        assertDeleteCompletionMarkRecord(id1, records.get(1));

        assertDeleteRecord(id2, records.get(2));
        assertDeleteCompletionMarkRecord(id2, records.get(3));

        assertDeleteRecord(id3, records.get(4));
        assertDeleteCompletionMarkRecord(id3, records.get(5));
    }

    private void assertDeleteRecord(String id, ProducerRecord<RecordKey, byte[]> deleteRecord) {
        assertEquals(
                new SecretBundleKey(CONNECTOR, id),
                deleteRecord.key()
        );
        assertNull(deleteRecord.value());
    }

    private void assertDeleteCompletionMarkRecord(String id, ProducerRecord<RecordKey, byte[]> deleteCompletionRecord) {
        assertEquals(
                new CompletionMarkKey(CONNECTOR, id),
                deleteCompletionRecord.key()
        );
        assertNull(deleteCompletionRecord.value());
    }

    private RecordMetadata recordMetadata(long offset) {
        return new RecordMetadata(TOPIC_PARTITION, offset, 0, 2L, 1, 2);
    }

    private void backfillSecrets(Map<String, String> connectorSecrets, String id) {
        ConsumerRecords<RecordKey, byte[]> records = secretsToRecords(connectorSecrets, id);
        secretStorage.processRecords(records);
    }

    private void deleteSecret(String id) {
        List<ConsumerRecord<RecordKey, byte[]>> records = Collections.singletonList(new ConsumerRecord<>(
                TOPIC, 0, 2L, new SecretBundleKey(CONNECTOR, id), null
        ));
        Map<TopicPartition, List<ConsumerRecord<RecordKey, byte[]>>> recordMap = new HashMap<>();
        recordMap.put(TOPIC_PARTITION, records);
        secretStorage.processRecords(new ConsumerRecords<>(recordMap));
    }

    private void setupIdentityCipherDecrypt() {
        expect(mockCipher.decrypt(anyObject())).andAnswer(() -> EasyMock.getCurrentArgument(0)).anyTimes();
        replay(mockCipher);
    }

    private void setupIdentityCipherEncrypt() {
        expect(mockCipher.encryptWithSignature(anyObject())).andAnswer(() -> EasyMock.getCurrentArgument(0));
        replay(mockCipher);
    }

    private void assertSecrets(Map<String, String> expectedSecrets, String id, boolean includeDeleted) {
        assertEquals(expectedSecrets, secretStorage.getSecrets(CONNECTOR, id, includeDeleted));
    }

    private ConsumerRecords<RecordKey, byte[]> secretsToRecords(Map<String, String> secrets, String id) {
        List<ConsumerRecord<RecordKey, byte[]>> records = Collections.singletonList(secretsToRecord(secrets, id));
        Map<TopicPartition, List<ConsumerRecord<RecordKey, byte[]>>> recordMap = new HashMap<>();
        recordMap.put(TOPIC_PARTITION, records);
        return new ConsumerRecords<>(recordMap);
    }

    private ConsumerRecord<RecordKey, byte[]> secretsToRecord(Map<String, String> secrets, String id) {
        byte[] payload;
        try {
            payload = objectMapper.writeValueAsBytes(new SecretValue(CONNECTOR, id, secrets));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new ConsumerRecord<>(TOPIC, 0, 1L, new SecretBundleKey(CONNECTOR, id), payload);
    }

    private SecretValue deserializeSecretValue(byte[] value) {
        try {
            return objectMapper.readValue(value, SecretValue.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
