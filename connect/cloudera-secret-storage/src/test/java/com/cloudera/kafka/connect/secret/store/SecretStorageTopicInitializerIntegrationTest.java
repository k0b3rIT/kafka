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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.cloudera.kafka.connect.secret.SecretCipher;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class SecretStorageTopicInitializerIntegrationTest extends IntegrationTestHarness {
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @Override
    public int brokerCount() {
        return 1;
    }

    @AfterEach
    public void cleanup() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        closeables.clear();
    }

    @Test
    public void testCandidatePickOrder() throws ExecutionException, InterruptedException {
        String topic = "test_candidate_order";
        SecretCipher mockCipher = mock(SecretCipher.class);
        Capture<byte[]> encryptionKeyCapture = newCapture();
        expect(mockCipher.initializeEncryptionKey(capture(encryptionKeyCapture))).andReturn(Optional.empty());
        replay(mockCipher);

        SecretStorageTopicInitializer initializer = createInitializer(topic, mockCipher);

        createSecretStorageTopic(topic);

        byte[] firstCandidate = new byte[] {1};
        byte[] secondCandidate = new byte[] {2};
        try (Producer<RecordKey, byte[]> candidateProducer = createProducer()) {
            candidateProducer.send(new ProducerRecord<>(topic, new EncryptionKeyCandidateKey("first_candidate"), firstCandidate));
            candidateProducer
                    .send(new ProducerRecord<>(topic, new EncryptionKeyCandidateKey("second_candidate"), secondCandidate))
                    .get();
        }

        initializer.initialize();

        verify(mockCipher);

        byte[] pickedCandidate = encryptionKeyCapture.getValue();
        assertArrayEquals(firstCandidate, pickedCandidate);
    }

    @Test
    public void testParallelInitializers() throws ExecutionException, InterruptedException {
        String topic = "test_parallel_init";
        int numberOfParallelInits = 20;

        List<Capture<byte[]>> encryptionKeyCaptures = IntStream
                .range(0, numberOfParallelInits)
                .mapToObj(i -> EasyMock.<byte[]>newCapture())
                .collect(Collectors.toList());
        List<SecretCipher> mockCiphers = encryptionKeyCaptures
                .stream()
                .map(capture -> {
                    SecretCipher mockCipher = mock(SecretCipher.class);
                    expect(mockCipher.initializeEncryptionKey(capture(capture))).andReturn(Optional.empty());
                    expect(mockCipher.generateEncryptionKey())
                            .andReturn(randomBytes())
                            .anyTimes();
                    replay(mockCipher);
                    return mockCipher;
                })
                .collect(Collectors.toList());
        List<SecretStorageTopicInitializer> initializers = mockCiphers
                .stream()
                .map(cipher -> createInitializer(topic, cipher))
                .collect(Collectors.toList());

        ExecutorService executor = Executors.newFixedThreadPool(numberOfParallelInits);
        try {
            List<Future<?>> initFutures = initializers
                    .stream()
                    .map(init -> executor.submit(init::initialize))
                    .collect(Collectors.toList());
            for (Future<?> future : initFutures) {
                future.get();
            }

            verify(mockCiphers.toArray(new Object[0]));

            byte[] encryptionKey = null;
            for (Capture<byte[]> keyCapture : encryptionKeyCaptures) {
                if (encryptionKey == null) {
                    encryptionKey = keyCapture.getValue();
                } else {
                    assertArrayEquals(encryptionKey, keyCapture.getValue(), "Encryption keys do not match");
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testEncryptionKeyUpdateCallback() {
        String topic = "update_callback";

        SecretCipher mockCipher = mock(SecretCipher.class);
        byte[] encKey1 = randomBytes();
        expect(mockCipher.generateEncryptionKey()).andReturn(encKey1);
        byte[] encKey2 = randomBytes();
        expect(mockCipher.initializeEncryptionKey(aryEq(encKey1))).andReturn(Optional.of(encKey2));
        mockCipher.encryptionKeySaved(aryEq(encKey2));
        replay(mockCipher);

        SecretStorageTopicInitializer initializer = createInitializer(topic, mockCipher);
        initializer.initialize();

        verify(mockCipher);
    }

    private byte[] randomBytes() {
        return UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    }

    private SecretStorageTopicInitializer createInitializer(String topic, SecretCipher mockCipher) {
        return new SecretStorageTopicInitializer(
                createProducer(),
                createConsumer(),
                mockCipher,
                new TopicPartition(topic, 0),
                Duration.ofSeconds(2),
                5000,
                adminConfigs(),
                Optional.empty(),
                Collections.emptyMap(),
                3,
                5000
        );
    }

    private void createSecretStorageTopic(String topic) {
        Properties topicProps = new Properties();
        topicProps.put("cleanup.policy", "compact");
        createTopic(topic, 1, 1, topicProps, listenerName(), new Properties());
    }

    private Producer<RecordKey, byte[]> createProducer() {
        Producer<RecordKey, byte[]> producer =
                createProducer(new StorageKeySerializer(), new ByteArraySerializer(), new Properties());
        closeables.add(producer);
        return producer;
    }

    private Consumer<RecordKey, byte[]> createConsumer() {
        Consumer<RecordKey, byte[]> consumer =
                new KafkaConsumer<>(adminConfigs(), new StorageKeyDeserializer(), new ByteArrayDeserializer());
        closeables.add(consumer);
        return consumer;
    }

    private Map<String, Object> adminConfigs() {
        Map<String, Object> adminConfigs = new HashMap<>();
        adminClientConfig().forEach((k, v) -> adminConfigs.put((String) k, v));
        return adminConfigs;
    }
}
