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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;

import com.cloudera.kafka.connect.secret.SecretCipher;
import com.cloudera.kafka.connect.secret.cipher.SecretCipherConfig;
import com.cloudera.kafka.connect.secret.store.CompletionMarkKey;
import com.cloudera.kafka.connect.secret.store.KafkaSecretStorage;
import com.cloudera.kafka.connect.secret.store.KafkaSecretStorageConfig;
import com.cloudera.kafka.connect.secret.store.RecordKey;
import com.cloudera.kafka.connect.secret.store.SecretBundleKey;
import com.cloudera.kafka.connect.secret.store.SecretBundleMap;
import com.cloudera.kafka.connect.secret.store.SecretStorageTopicInitializer;
import com.cloudera.kafka.connect.secret.store.SecretValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReEncryptionTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReEncryptionTool.class);

    private static final byte[] COMPLETED_MARK_MAGIC_CONTENT = "ok".getBytes(StandardCharsets.UTF_8);

    private final ObjectMapper objectMapper;
    private final Map<String, Object> sourceConfig;
    private final Map<String, Object> targetConfig;

    public ReEncryptionTool(Map<String, Object> sourceConfig, Map<String, Object> targetConfig) {
        this.sourceConfig = sourceConfig;
        this.targetConfig = targetConfig;
        objectMapper = new ObjectMapper();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3 || Arrays.stream(args).anyMatch(a -> "--help".equals(a) || "-h".equals(a))) {
            LOGGER.info(
                    "Kafka Connect Secret Storage re-encryption and migration tool."
                            + " Migrates secrets of an existing storage topic to a new topic,"
                            + " re-encrypting them with a newly generated encryption key.\n"
                            + "NOTE: Kafka Connect cluster MUST be stopped during migration!\n"
                            + "Usage: {} kafka.properties current_secret_storage.properties new_secret_storage.properties\n"
                            + "  kafka.properties - file containing the Kafka connection properties\n"
                            + "  current_secret_storage.properties - current secret storage properties\n"
                            + "  new_secret_storage.properties - new secret storage properties",
                    ReEncryptionTool.class.getSimpleName());
            Exit.exit(1);
        }

        Map<String, Object> kafkaConfig = readProperties(args[0]);
        Map<String, Object> sourceConfig = readProperties(args[1]);
        Map<String, Object> targetConfig = readProperties(args[2]);

        sourceConfig.putAll(kafkaConfig);
        targetConfig.putAll(kafkaConfig);

        if (!sourceConfig.containsKey(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG)) {
            String sourceGlobalPassword =
                    readPassword("Please provide the global password of the current secret storage topic:");
            sourceConfig.put(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, sourceGlobalPassword);
        }
        if (!targetConfig.containsKey(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG)) {
            String targetGlobalPassword =
                    readPassword("Please provide the global password of the new secret storage topic"
                            + " - make sure to be able to provide this password in the Kafka Connect configuration"
                            + " after the migration is finished:");
            targetConfig.put(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, targetGlobalPassword);
        }

        LOGGER.warn("About to start secret re-encryption. Please make sure that the Kafka Connect cluster is stopped");
        LOGGER.info("Press Enter to continue.");
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
        scanner.nextLine();

        try {
            ReEncryptionTool tool = new ReEncryptionTool(sourceConfig, targetConfig);
            tool.run();
        } catch (Exception e) {
            LOGGER.error("Secret re-encryption and migration failed"
                    + " - the target topic is not suitable for secret storage use, and can be deleted."
                    + " Fix the underlying issue, then retry the operation", e);
            Exit.exit(1);
        }

        LOGGER.info("Secret migration finished successfully. Please re-configure the Kafka Connect cluster to use"
                + " the new secret storage topic, the new global password,"
                + " and all related configurations of the new global key");
        LOGGER.info("After the reconfiguration, verify that the Kafka Connect cluster is working as expected");
        LOGGER.info("Only delete the old secret storage topic when the correctness of the new topic was verified");
    }

    private static String readPassword(String message) {
        LOGGER.info(message);
        Console console = System.console();
        if (console != null) {
            return new String(System.console().readPassword());
        }
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
        return scanner.nextLine();
    }

    private static Map<String, Object> readProperties(String file) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            props.load(fis);
        }
        Map<String, Object> configMap = new HashMap<>();
        props.forEach((key, value) -> configMap.put((String) key, value));
        return configMap;
    }

    public void run() {
        KafkaSecretStorageConfig config = new KafkaSecretStorageConfig(targetConfig);
        SecretCipher cipher = config.getSecretCipher();

        initializeTargetTopic(config, cipher);
        SecretBundleMap<Map<String, String>> bundles = readSecretsFromSource();
        sendSecretsToTargetTopic(config, bundles, cipher);
    }

    private void initializeTargetTopic(KafkaSecretStorageConfig config, SecretCipher cipher) {
        LOGGER.info("Checking if target topic {} exists", config.getTopic());
        try (Admin admin = Admin.create(config.getAdminConfigs())) {
            boolean exists = admin.listTopics().names().get().contains(config.getTopic());
            if (exists) {
                throw new RuntimeException("The target topic already exists, please use a non-existent topic"
                        + " as the new secret storage topic");
            }
        } catch (ExecutionException e) {
            throw new RuntimeException("Exception while checking target topic", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while checking target topic", e);
        }

        LOGGER.info("Initializing target topic {}", config.getTopic());
        try (Consumer<RecordKey, byte[]> consumer = new KafkaConsumer<>(config.getConsumerConfigs());
            Producer<RecordKey, byte[]> producer = new KafkaProducer<>(config.getProducerConfigs())) {
            SecretStorageTopicInitializer initializer = new SecretStorageTopicInitializer(producer, consumer, cipher,
                    new TopicPartition(config.getTopic(), 0),
                    Duration.ofMillis(config.getSecretConsumerStartupPollDurationMs()), 0L,
                    config.getAdminConfigs(), config.getTopicReplicationFactor(), config.getTopicConfigs(),
                    config.getTopicCreateRetries(), config.getTopicCreateRetryBackoffMs());
            initializer.initialize();
        }
    }

    private SecretBundleMap<Map<String, String>> readSecretsFromSource() {
        LOGGER.info("Reading secrets from source topic");
        try (KafkaSecretStorage sourceStorage = new KafkaSecretStorage()) {
            // configure is expected to read to the end of the topic-partition
            // given that there are producers writing into it
            sourceStorage.configure(sourceConfig);
            return sourceStorage.getConnectorSecrets();
        }
    }

    private void sendSecretsToTargetTopic(KafkaSecretStorageConfig config, SecretBundleMap<Map<String, String>> bundles,
                                          SecretCipher cipher) {
        String topic = config.getTopic();
        try (Producer<RecordKey, byte[]> producer = new KafkaProducer<>(config.getProducerConfigs())) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            bundles.forEach((connector, connectorBundles) -> {
                LOGGER.info("Migrating secrets of connector {}", connector);
                List<Future<RecordMetadata>> connectorFutures = connectorBundles
                        .entrySet()
                        .stream()
                        .flatMap(e -> {
                            if (e.getValue().isDeleted()) {
                                // Bundle is only here due to delayed delete, let's not transfer
                                return Stream.of();
                            }
                            byte[] value = encryptedValue(cipher, connector, e.getKey(), e.getValue().getSecrets());
                            List<MigratedRecordWithSourceOffset> records = new ArrayList<>();
                            records.add(new MigratedRecordWithSourceOffset(
                                    new ProducerRecord<>(topic, 0, e.getValue().getTimestamp(),
                                            new SecretBundleKey(connector, e.getKey()), value),
                                    e.getValue().getOffset()
                            ));
                            if (e.getValue().isCompleted()) {
                                records.add(new MigratedRecordWithSourceOffset(
                                        new ProducerRecord<>(topic, 0, e.getValue().getTimestamp(),
                                                new CompletionMarkKey(connector, e.getKey()),
                                                COMPLETED_MARK_MAGIC_CONTENT),
                                        e.getValue().getCompletedAtOffset()
                                ));
                            }
                            return records.stream();
                        })
                        .sorted()
                        .map(MigratedRecordWithSourceOffset::getRecord)
                        .map(producer::send)
                        .collect(Collectors.toList());
                LOGGER.info("Found {} secret bundles to be migrated for connector {}",
                        connectorFutures.size(), connector);
                futures.addAll(connectorFutures);
            });

            producer.flush();

            futures.forEach(future -> {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for producer send futures", e);
                } catch (ExecutionException e) {
                    throw new RuntimeException("Exception while waiting for producer send futures", e);
                }
            });
        }
    }

    private byte[] encryptedValue(SecretCipher cipher, String connector, String id, Map<String, String> secrets) {
        SecretValue value = new SecretValue(connector, id, secrets);
        byte[] serializedValue;
        try {
            serializedValue = objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not serialize secret values for connector " + connector + " id " + id, e);
        }
        try {
            return cipher.encryptWithSignature(serializedValue);
        } catch (Exception e) {
            throw new RuntimeException("Failed to encrypt secret values for connector " + connector + " id " + id, e);
        }
    }

    static class MigratedRecordWithSourceOffset implements Comparable<MigratedRecordWithSourceOffset> {
        final ProducerRecord<RecordKey, byte[]> record;
        final long sourceOffset;

        MigratedRecordWithSourceOffset(ProducerRecord<RecordKey, byte[]> record, long sourceOffset) {
            this.record = record;
            this.sourceOffset = sourceOffset;
        }

        ProducerRecord<RecordKey, byte[]> getRecord() {
            return record;
        }

        @Override
        public int compareTo(MigratedRecordWithSourceOffset o) {
            return Long.compare(sourceOffset, o.sourceOffset);
        }
    }
}
