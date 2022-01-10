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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;

import com.cloudera.kafka.connect.secret.SecretCipher;
import com.cloudera.kafka.connect.secret.SecretCipherException;
import com.cloudera.kafka.connect.secret.SecretStorage;
import com.cloudera.kafka.connect.secret.SecretStorageException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implements {@link SecretStorage} by using a single-partition Kafka topic.
 * The topic and the encryption key is initialized using {@link SecretStorageTopicInitializer}.
 * Records written to the topic use the following keys:
 * {@link EncryptionKeyCandidateKey} used for encryption candidate keys,
 * {@link EncryptionKey} used for encryption keys,
 * {@link SecretBundleKey} used for secret keys,
 * {@link CompletionMarkKey} use for marking secret updates as finished.
 * This implementation detects concurrent modifications on the same connector when a secret record was not deleted,
 * was not marked for completion, and was not older than a configurable age.
 */
public class KafkaSecretStorage implements SecretStorage, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSecretStorage.class);

    private static final byte[] COMPLETED_MARK_MAGIC_CONTENT = "ok".getBytes(StandardCharsets.UTF_8);

    private final SecretBundleMap<Map<String, String>> connectorSecrets;
    private final ObjectMapper objectMapper;
    private final Object lock;
    private SecretCipher cipher;
    private String topic;
    private TopicPartition secretStoragePartition;
    private Consumer<RecordKey, byte[]> consumer;
    private Producer<RecordKey, byte[]> producer;
    private long timeoutForOperations;
    private Duration pollDuration;
    private Duration startupPollDuration;
    private Duration producerCloseTimeoutDuration;
    private long secretDeleteDelayMs;
    private long concurrentModificationTimeoutMs;
    private ExecutorService consumerExecutorService;
    private ScheduledExecutorService deleteExecutorService;
    private volatile boolean running;

    public KafkaSecretStorage() {
        connectorSecrets = new SecretBundleMap<>();
        objectMapper = new ObjectMapper();
        lock = new Object();
        running = true;
    }

    //Visible for testing
    @SuppressWarnings("unused")
    KafkaSecretStorage(SecretCipher cipher, String topic, TopicPartition secretStoragePartition,
                       Consumer<RecordKey, byte[]> consumer, Producer<RecordKey, byte[]> producer,
                       long timeoutForOperations, Duration pollDuration, Duration startupPollDuration,
                       Duration producerCloseTimeoutDuration, long secretDeleteDelayMs,
                       long concurrentModificationTimeoutMs, ScheduledExecutorService deleteExecutorService) {
        this();
        this.cipher = cipher;
        this.topic = topic;
        this.secretStoragePartition = secretStoragePartition;
        this.consumer = consumer;
        this.producer = producer;
        this.timeoutForOperations = timeoutForOperations;
        this.pollDuration = pollDuration;
        this.startupPollDuration = startupPollDuration;
        this.producerCloseTimeoutDuration = producerCloseTimeoutDuration;
        this.secretDeleteDelayMs = secretDeleteDelayMs;
        this.concurrentModificationTimeoutMs = concurrentModificationTimeoutMs;
        this.deleteExecutorService = deleteExecutorService;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        KafkaSecretStorageConfig config = new KafkaSecretStorageConfig(configs);
        cipher = config.getSecretCipher();
        topic = config.getTopic();
        timeoutForOperations = config.getSecretOperationTimeoutMs();
        pollDuration = Duration.ofMillis(config.getSecretConsumerPollDurationMs());
        startupPollDuration = Duration.ofMillis(config.getSecretConsumerStartupPollDurationMs());
        producerCloseTimeoutDuration = Duration.ofMillis(config.getSecretProducerCloseTimeoutMs());
        secretDeleteDelayMs = config.getSecretDeleteDelayMs();
        concurrentModificationTimeoutMs = config.getSecretConcurrentModificationTimeoutMs();
        secretStoragePartition = new TopicPartition(topic, 0);

        synchronized (lock) {
            if (!running) {
                LOGGER.warn("Already closed, not initializing");
                return;
            }
            try {
                consumerExecutorService = Executors.newSingleThreadExecutor();
                deleteExecutorService = Executors.newSingleThreadScheduledExecutor();
                initializeTopicAndCipher(config);
            } catch (Throwable t) {
                running = false;
                if (consumer != null) {
                    consumer.close(Duration.ZERO);
                }
                if (producer != null) {
                    producer.close(Duration.ZERO);
                }
                LOGGER.error("Failed to initialize", t);
                throw t;
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            running = false;
            if (consumer != null) {
                consumer.wakeup();
            }
            shutdownExecutorService(consumerExecutorService);
            shutdownExecutorService(deleteExecutorService);
            if (producer != null) {
                producer.close(producerCloseTimeoutDuration);
            }
        }
    }

    @Override
    public Map<String, String> getSecrets(String connector, String id, boolean includeLingeringDeleted) {
        long deadline = System.currentTimeMillis() + timeoutForOperations;
        synchronized (lock) {
            LOGGER.trace("Trying to fetch bundle {} for connector {}.", id, connector);
            if (!waitForConnectorId(connector, id, deadline)) {
                return null;
            }

            SecretBundle<Map<String, String>> bundle = connectorSecrets.getBundle(connector, id);
            if (bundle == null) {
                return null;
            }
            if (bundle.isDeleted() && !includeLingeringDeleted) {
                return null;
            }
            LOGGER.trace("Found bundle {} for connector {}.", id, connector);
            return new HashMap<>(bundle.getSecrets());
        }
    }

    @Override
    public String saveSecrets(String connector, Map<String, String> secretsToSave) {
        if (secretsToSave == null || secretsToSave.isEmpty()) {
            throw new IllegalArgumentException("Secrets must not be empty");
        }

        long deadline = System.currentTimeMillis() + timeoutForOperations;
        String id = UUID.randomUUID().toString();
        LOGGER.info("Generated id {} for new secret bundle for connector {}", id, connector);
        RecordMetadata metadata = sendSecretUpdate(connector, id, secretsToSave, deadline);
        try {
            if (checkVersionIsConcurrentlyModified(connector, id, deadline, metadata.timestamp(), metadata.offset())) {
                // The connector is being concurrently modified, let's refuse the change
                throw new ConcurrentModificationException("The secrets of connector " + connector
                        + " are currently being modified");
            }
            return id;
        } catch (Throwable t) {
            deleteSecretsVersion(connector, id);
            throw t;
        }
    }

    @Override
    public void deleteSecrets(String connector, String id) {
        LOGGER.debug("Deleting secrets of connector {} in bundle {}", connector, id);
        deleteSecretsVersion(connector, id);
    }

    @Override
    public void deletePreviousSecrets(String connector, String id) {
        long deadline = System.currentTimeMillis() + timeoutForOperations;
        deletePreviousSecrets(connector, id, deadline);
    }

    @Override
    public void deleteSecretsAndPreviousSecrets(String connector, String id) {
        long deadline = System.currentTimeMillis() + timeoutForOperations;
        deletePreviousSecrets(connector, id, deadline);
        deleteSecretsVersion(connector, id);
    }

    @Override
    public void markForCompletionAndDeletePrevious(String connector, String id) {
        LOGGER.debug("Marking {} connector bundle {} complete", connector, id);
        long deadline = System.currentTimeMillis() + timeoutForOperations;
        producer.send(convertToCompletedMarkRecord(connector, id));
        deletePreviousSecrets(connector, id, deadline);
    }

    public SecretBundleMap<Map<String, String>> getConnectorSecrets() {
        synchronized (lock) {
            return connectorSecrets.copy();
        }
    }

    private void deletePreviousSecrets(String connector, String id, long deadline) {
        List<SecretBundle<Map<String, String>>> previousVersions =
                findPreviousNonDeletedBundles(connector, id, deadline);
        previousVersions.forEach(prevBundle -> deleteSecretsVersion(connector, prevBundle.getId()));
    }

    private void shutdownExecutorService(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        boolean shutdown = false;
        try {
            shutdown = executor.awaitTermination(2 * pollDuration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!shutdown) {
            LOGGER.warn("Executor shutdown timed out, closing forcefully");
            executor.shutdownNow();
        }
    }

    private RecordMetadata sendSecretUpdate(String connector, String id, Map<String, String> secretsToSave, long deadline) {
        ProducerRecord<RecordKey, byte[]> record = convertSecretsToRecord(connector, id, secretsToSave);
        Future<RecordMetadata> future = producer.send(record);
        producer.flush();
        RecordMetadata metadata = waitForSend(future,
                deadline,
                String.format("saving secrets for connector %s id %s", connector, id),
                SecretStorageException::new);
        LOGGER.info("Bundle for connector {} id {} has offset {}", connector, id, metadata.offset());
        return metadata;
    }

    private List<SecretBundle<Map<String, String>>> findPreviousNonDeletedBundles(String connector, String id, long deadline) {
        return findPreviousBundles(connector, id, deadline)
                .stream()
                .filter(bundle -> !bundle.isDeleted())
                .collect(Collectors.toList());
    }

    private List<SecretBundle<Map<String, String>>> findPreviousBundles(String connector, String id, long deadline) {
        synchronized (lock) {
            waitForConnectorIdThrowOnTimeout(connector, id, deadline);
            List<SecretBundle<Map<String, String>>> previousVersions =
                    connectorSecrets.findPreviousBundles(connector, id);
            return previousVersions == null ? Collections.emptyList() : previousVersions;
        }
    }

    private void waitForConnectorIdThrowOnTimeout(String connector, String id, long deadline) {
        if (!waitForConnectorId(connector, id, deadline)) {
            throw new SecretStorageException("Timed out while waiting for storage updates");
        }
    }

    // Visible for testing
    boolean waitForConnectorId(String connector, String id, long deadline) {
        try {
            while (!isConnectorBundlePresent(connector, id)) {
                long now = System.currentTimeMillis();
                if (now >= deadline) {
                    return false;
                }
                lock.wait(deadline - now);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SecretStorageException("Interrupted while waiting for storage updates", e);
        }
        return true;
    }

    private boolean isConnectorBundlePresent(String connector, String id) {
        return connectorSecrets.getBundle(connector, id) != null;
    }

    private void deleteSecretsVersion(String connector, String id) {
        LOGGER.trace("Deleting {} connector's bundle : {}", connector, id);
        producer.send(new ProducerRecord<>(topic, 0, new SecretBundleKey(connector, id), null));
        producer.send(new ProducerRecord<>(topic, 0, new CompletionMarkKey(connector, id), null));
    }

    private boolean checkVersionIsConcurrentlyModified(String connector, String id, long deadline, long recordTimestamp,
                                                       long recordOffset) {
        synchronized (lock) {
            waitForConnectorIdThrowOnTimeout(connector, id, deadline);
            List<SecretBundle<Map<String, String>>> previousBundles =
                    connectorSecrets.findPreviousBundles(connector, id);

            if (previousBundles == null) {
                // Null means that there are no valid bundles in the store, even after we waited for the ID to appear
                // Should never happen
                LOGGER.error("No bundles available for connector {} after" +
                        " waiting for id {} to appear", connector, id);
                return false;
            }
            return previousBundles
                    .stream()
                    .anyMatch(bundle ->
                            !bundle.isFinalized(recordTimestamp - concurrentModificationTimeoutMs, recordOffset));
        }
    }

    private ProducerRecord<RecordKey, byte[]> convertSecretsToRecord(String connector,
                                                                     String id,
                                                                     Map<String, String> secrets) {
        byte[] secretsSerialized;
        try {
            SecretValue value = new SecretValue(connector, id, secrets);
            secretsSerialized = objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize secrets for connector {} id {}", connector, id, e);
            throw new SecretStorageException("Failed to serialize secrets", e);
        }

        try {
            byte[] secretsEncrypted = cipher.encryptWithSignature(secretsSerialized);
            return new ProducerRecord<>(topic, 0, new SecretBundleKey(connector, id), secretsEncrypted);
        } catch (SecretCipherException e) {
            LOGGER.error("Failed to encrypt secrets for connector {} id {}", connector, id, e);
            throw new SecretStorageException("Error while encrypting secrets", e);
        }
    }

    private ProducerRecord<RecordKey, byte[]> convertToCompletedMarkRecord(String connector, String id) {
        return new ProducerRecord<>(topic, 0, new CompletionMarkKey(connector, id),
                COMPLETED_MARK_MAGIC_CONTENT);
    }

    private void initializeTopicAndCipher(KafkaSecretStorageConfig config) {
        producer = createProducer(config);
        consumer = createConsumer(config);

        SecretStorageTopicInitializer initializer = new SecretStorageTopicInitializer(
                producer, consumer, cipher, secretStoragePartition, startupPollDuration, secretDeleteDelayMs,
                config.getAdminConfigs(), config.getTopicReplicationFactor(), config.getTopicConfigs(),
                config.getTopicCreateRetries(), config.getTopicCreateRetryBackoffMs()
        );
        initializer.initialize();

        initializer.getUnencryptedSecrets().forEach(
                (connector, bundles) -> bundles.forEach(
                        (id, bundle) -> {
                            processSecretRecord(new SecretBundleKey(connector, bundle.getId()),
                                    bundle.getSecrets(), bundle.getOffset(), bundle.getTimestamp());
                            if (bundle.isCompleted()) {
                                connectorSecrets.markBundleCompleted(connector, id, bundle.getCompletedAtOffset());
                            }
                            if (bundle.isDeleted()) {
                                connectorSecrets.markBundleDeleted(connector, id, bundle.getDeletedAtOffset());
                            }
                        }
                )
        );
        Set<SecretBundleKey> secretsToDeleteWithDelay = initializer.getSecretsToDelete();
        if (!secretsToDeleteWithDelay.isEmpty()) {
            deleteExecutorService.schedule(() -> {
                synchronized (lock) {
                    secretsToDeleteWithDelay.forEach(key -> removeSecrets(key.getConnector(), key.getId()));
                }
            }, secretDeleteDelayMs, TimeUnit.MILLISECONDS);
        }

        consumerExecutorService.submit(this::runConsumer);
    }

    private Consumer<RecordKey, byte[]> createConsumer(KafkaSecretStorageConfig config) {
        return new KafkaConsumer<>(config.getConsumerConfigs());
    }

    private Producer<RecordKey, byte[]> createProducer(KafkaSecretStorageConfig config) {
        return new KafkaProducer<>(config.getProducerConfigs());
    }

    private void runConsumer() {
        try {
            while (running) {
                try {
                    ConsumerRecords<RecordKey, byte[]> records = consumer.poll(pollDuration);
                    processRecords(records);
                } catch (WakeupException | InterruptException e) {
                    // Ignore
                } catch (RetriableException e) {
                    LOGGER.warn("Caught retriable consumer exception, keep consuming", e);
                } catch (Throwable t) {
                    LOGGER.error("Error occurred in background consumer thread, keep consuming", t);
                }
            }
        } finally {
            consumer.close(Duration.ZERO);
        }
    }

    //Visible for testing
    void processRecords(ConsumerRecords<RecordKey, byte[]> records) {
        if (records.isEmpty()) {
            return;
        }
        synchronized (lock) {
            records.forEach(this::processRecord);
            lock.notifyAll();
        }
    }

    private void processRecord(ConsumerRecord<RecordKey, byte[]> consumerRecord) {
        RecordKey recordKey = consumerRecord.key();
        if (recordKey == null) {
            return;
        }
        try {
            if (recordKey instanceof SecretBundleKey) {
                processSecretRecord((SecretBundleKey) recordKey, consumerRecord.value(), consumerRecord.offset(),
                        consumerRecord.timestamp());
            } else if (recordKey instanceof CompletionMarkKey) {
                processCompletionMarkRecord((CompletionMarkKey) recordKey, consumerRecord.value(),
                        consumerRecord.offset());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to process record with key {} at offset {}", recordKey, consumerRecord.offset());
        }
    }

    private void processCompletionMarkRecord(CompletionMarkKey recordKey, byte[] value, long offset) {
        if (value == null) {
            // We can ignore delete records for the completion marker
            return;
        }
        markSecretForCompletion(recordKey.getConnector(), recordKey.getId(), offset);
    }

    private void processSecretRecord(SecretBundleKey recordKey, byte[] value, long offset, long timestamp) {
        if (value == null) {
            // Deletion shows that the secret is already "completed", but the removal is delayed
            // We need to mark the secret so new edits do not encounter concurrent modification errors
            markSecretForDeletion(recordKey.getConnector(), recordKey.getId(), offset);
            deleteExecutorService.schedule(
                    () -> removeSecrets(recordKey.getConnector(), recordKey.getId()),
                    secretDeleteDelayMs, TimeUnit.MILLISECONDS
            );
        } else {
            try {
                byte[] decryptedValue = cipher.decrypt(value);
                SecretValue secretsFromRecord = objectMapper.readValue(decryptedValue, SecretValue.class);
                if (!recordKey.getConnector().equals(secretsFromRecord.getConnector())) {
                    LOGGER.warn("Encountered invalid secret record for connector {}" +
                                    " - Connector mismatching: connector from value {}",
                            recordKey.getConnector(), recordKey.getConnector());
                }
                if (!recordKey.getId().equals(secretsFromRecord.getId())) {
                    LOGGER.warn("Encountered invalid secret record for connector {}" +
                            " - ID mismatching: id from key {} id from value {}",
                            recordKey.getConnector(), recordKey.getId(), secretsFromRecord.getId());
                }
                addSecrets(recordKey.getConnector(), offset, recordKey.getId(),
                        timestamp, secretsFromRecord.getSecrets());
            } catch (IOException e) {
                LOGGER.error("Could not deserialize record value for key {} at offset {}", recordKey, offset, e);
                throw new SecretStorageException("Failed to deserialize record", e);
            } catch (SecretCipherException e) {
                LOGGER.error("Could not decrypt secrets for key {} at offset {}", recordKey, offset, e);
                throw new SecretStorageException("Failed to decrypt secrets", e);
            }
        }
    }

    private void addSecrets(String connector, long version, String id, long timestamp, Map<String, String> secrets) {
        SecretBundle<Map<String, String>> bundle = new SecretBundle<>(connector, id, version, timestamp, secrets);
        if (connectorSecrets.addBundle(bundle)) {
            // Should never happen, so better log it
            LOGGER.error("When saving the secrets for connector {} version {}, a previous entry was overridden",
                    connector, version);
        }
    }

    private void markSecretForCompletion(String connector, String id, long offset) {
        LOGGER.trace("Marking connector {} secrets as completed with id {}", connector, id);
        if (!connectorSecrets.markBundleCompleted(connector, id, offset)) {
            LOGGER.debug("Tried marking secrets as completed for connector {} id {}," +
                    " but could not find the record", connector, id);
        }
    }

    private void markSecretForDeletion(String connector, String id, long offset) {
        LOGGER.trace("Marking connector {} secrets as deleted with id {}", connector, id);
        if (!connectorSecrets.markBundleDeleted(connector, id, offset)) {
            LOGGER.debug("Tried marking secrets as deleted for connector {} id {}," +
                    " but could not find the record", connector, id);
        }
    }

    private void removeSecrets(String connector, String id) {
        LOGGER.trace("Removing connector {} secrets with id {}", connector, id);
        if (!connectorSecrets.removeBundle(connector, id)) {
            LOGGER.trace("Tried removing secrets for connector {} id {} but could not find the record", connector, id);
        }
    }

    private RecordMetadata waitForSend(Future<RecordMetadata> future, long deadline, String description,
                             Function<Throwable, ? extends RuntimeException> exceptionWrapper) {
        return KafkaClientAction.executeWrapped(
                () -> {
                    if (timeoutForOperations == 0) {
                        return future.get();
                    } else {
                        return future.get(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    }
                },
                description,
                exceptionWrapper
        );
    }
}
