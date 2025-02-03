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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.TopicAdmin;

import com.cloudera.kafka.connect.secret.SecretCipher;
import com.cloudera.kafka.connect.secret.SecretCipherException;
import com.cloudera.kafka.connect.secret.SecretStorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Responsible for creating the single partition secret storage topic, and to generate and select an encryption key
 * when the topic is created.
 * Initializes the {@link SecretCipher} and updates the encryption key in the topic when necessary.
 */
public class SecretStorageTopicInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecretStorageTopicInitializer.class);

    private final Producer<RecordKey, byte[]> producer;
    private final Consumer<RecordKey, byte[]> consumer;
    private final SecretCipher cipher;
    private final TopicPartition topicPartition;
    private final Duration pollDuration;
    private final long secretDeleteDelayMs;
    private final Map<String, Object> adminConfigs;
    private final Optional<Short> replicationFactor;
    private final Map<String, Object> topicConfigs;
    private final int topicCreateRetries;
    private final int topicCreateBackoffMs;
    private final long adminTimeoutMs;
    private final Map<EncryptionKeyCandidateKey, byte[]> candidates;
    private final Set<Integer> encryptionKeyVersions;
    private final SecretBundleMap<byte[]> unencryptedSecrets;
    private final Set<SecretBundleKey> secretsToDelete;

    private State state;
    private long lastEncryptionKeyOffset;
    private EncryptionKey lastEncryptionKeyRecordKey;
    private byte[] lastEncryptionKeyRecordValue;

    public SecretStorageTopicInitializer(Producer<RecordKey, byte[]> producer, Consumer<RecordKey, byte[]> consumer,
                                         SecretCipher cipher, TopicPartition topicPartition, Duration pollDuration,
                                         long secretDeleteDelayMs, Map<String, Object> adminConfigs,
                                         Optional<Short> replicationFactor, Map<String, Object> topicConfigs,
                                         int topicCreateRetries, int topicCreateBackoffMs) {
        this.producer = producer;
        this.consumer = consumer;
        this.cipher = cipher;
        this.topicPartition = topicPartition;
        this.pollDuration = pollDuration;
        this.secretDeleteDelayMs = secretDeleteDelayMs;
        this.adminConfigs = adminConfigs;
        this.replicationFactor = replicationFactor;
        this.topicConfigs = topicConfigs;
        this.topicCreateRetries = topicCreateRetries;
        this.topicCreateBackoffMs = topicCreateBackoffMs;
        this.adminTimeoutMs = Long.parseLong(adminConfigs
                .getOrDefault(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000")
                .toString());
        candidates = new LinkedHashMap<>();
        encryptionKeyVersions = new TreeSet<>(Collections.reverseOrder());
        unencryptedSecrets = new SecretBundleMap<>();
        secretsToDelete = new HashSet<>();

        state = State.UNINITIALIZED;
        lastEncryptionKeyOffset = -1;
        lastEncryptionKeyRecordKey = null;
        lastEncryptionKeyRecordValue = null;
    }

    public void initialize() {
        createTopicEnsureConfig();

        consumer.assign(Collections.singleton(topicPartition));
        consumer.seekToBeginning(Collections.emptySet());

        // Try to initialize the cipher - init might take 3 steps
        // 1. Produce encryption key candidate into empty topic
        // 2. Pick first candidate and produce it as an encryption key with version 1
        // 3. Pick up the last encryption key from the topic
        // Most of the time, there will be a single step, as the encryption key will be found on the first run
        State previousState;
        while (state != State.FOUND_ENCRYPTION_KEY) {
            List<ConsumerRecord<RecordKey, byte[]>> records = readToEnd();
            long now = System.currentTimeMillis();
            records.forEach(record -> processRecord(record, now));

            previousState = state;
            state = updateState();
            if (!previousState.isValidNextState(state)) {
                LOGGER.error("Invalid state transition from {} to {}", previousState, state);
                throw new SecretStorageException("Failed to initialize secret storage - invalid state transition from "
                        + previousState + " to " + state);
            }
            LOGGER.info("State transitioned from {} to {}", previousState, state);
        }
    }

    public SecretBundleMap<byte[]> getUnencryptedSecrets() {
        return unencryptedSecrets;
    }

    public Set<SecretBundleKey> getSecretsToDelete() {
        return secretsToDelete;
    }

    private State updateState() {
        if (lastEncryptionKeyRecordKey != null && lastEncryptionKeyRecordValue != null) {
            LOGGER.info("Found encryption key at offset {} with version {}, initializing cipher",
                    lastEncryptionKeyOffset, lastEncryptionKeyRecordKey.getVersion());
            Optional<byte[]> updatedEncryptionKey;
            try {
                updatedEncryptionKey = cipher.initializeEncryptionKey(lastEncryptionKeyRecordValue);
            } catch (SecretCipherException e) {
                throw new SecretStorageException("Failed to initialize cipher", e);
            }

            if (updatedEncryptionKey.isPresent()) {
                int nextVersion = lastEncryptionKeyRecordKey.getVersion() + 1;
                LOGGER.info("Global key was updated, updating encryption key to version {}", nextVersion);
                sendEncryptionKey(nextVersion, updatedEncryptionKey.get());
                cipher.encryptionKeySaved(updatedEncryptionKey.get());
            } else {
                // Make sure that we don't "clean up" the current encryption key
                encryptionKeyVersions.remove(lastEncryptionKeyRecordKey.getVersion());
            }

            cleanupEncryptionKeyCandidates();
            cleanupEncryptionKeys();

            return State.FOUND_ENCRYPTION_KEY;
        }

        if (candidates.isEmpty()) {
            // We need to try to create an encryption key
            // This is done by first producing a candidate, then reading again to find the first candidate
            sendCandidate();
            return State.CANDIDATE_PRODUCED;
        }

        // Pick the first candidate
        Map.Entry<EncryptionKeyCandidateKey, byte[]> selectedCandidate = candidates.entrySet().iterator().next();
        LOGGER.info("Picked encryption key candidate with id {} from candidates {}",
                selectedCandidate.getKey().getId(), candidates.keySet());
        sendEncryptionKey(1, selectedCandidate.getValue());
        return State.ENCRYPTION_KEY_CANDIDATE_PROMOTED;
    }

    private void sendEncryptionKey(int version, byte[] payload) {
        RecordKey key = new EncryptionKey(version);
        LOGGER.info("Sending encryption key version {}", version);
        long offset = KafkaClientAction.executeWrapped(
                () -> {
                    Future<RecordMetadata> sendFuture = producer
                            .send(new ProducerRecord<>(topicPartition.topic(), 0, key, payload));
                    producer.flush();
                    return sendFuture.get();
                },
                "saving encryption key",
                t -> new SecretStorageException("Error while saving the encryption key with version " + version, t)
        ).offset();
        LOGGER.info("Saved encryption key version {} at offset {}", version, offset);
    }

    private void sendCandidate() {
        EncryptionKeyCandidateKey candidateRecordKey = new EncryptionKeyCandidateKey(UUID.randomUUID().toString());
        LOGGER.info("Sending encryption key candidate with id {}", candidateRecordKey.getId());
        byte[] candidateKey;
        try {
            candidateKey = cipher.generateEncryptionKey();
        } catch (SecretCipherException e) {
            throw new SecretStorageException("Failed to generate encryption key", e);
        }
        long offset = KafkaClientAction.executeWrapped(
                () -> {
                    Future<RecordMetadata> sendFuture = producer
                            .send(new ProducerRecord<>(topicPartition.topic(), 0, candidateRecordKey, candidateKey));
                    producer.flush();
                    return sendFuture.get();
                },
                "sending encryption key candidate with id " + candidateRecordKey.getId(),
                t -> new SecretStorageException("Failed to encryption key candidate with id " + candidateRecordKey.getId(), t)
        ).offset();
        LOGGER.info("Saved encryption key candidate with id {} at offset {}", candidateRecordKey.getId(), offset);
    }

    private void cleanupEncryptionKeyCandidates() {
        // Delete candidates in reverse order to avoid corner cases around log compaction
        // We always use the first candidate as the encryption key, let's delete that last
        ListIterator<EncryptionKeyCandidateKey> reverseIterator = new ArrayList<>(candidates.keySet())
                .listIterator(candidates.size());
        while (reverseIterator.hasPrevious()) {
            EncryptionKeyCandidateKey recordKey = reverseIterator.previous();
            // No need to wait for the futures of the deletion records, this is a best-effort operation
            producer.send(new ProducerRecord<>(topicPartition.topic(), 0, recordKey, null));
        }
    }

    private void cleanupEncryptionKeys() {
        // No need to wait for the futures of the deletion records, this is a best-effort operation
        encryptionKeyVersions.forEach(v ->
                producer.send(new ProducerRecord<>(topicPartition.topic(), 0, new EncryptionKey(v), null)));
    }

    private List<ConsumerRecord<RecordKey, byte[]>> readToEnd() {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singleton(topicPartition));
        Long partitionEndOffset = endOffsets.get(topicPartition);
        if (partitionEndOffset == null) {
            LOGGER.warn("Could not fetch the end offset of {}, partition seems to be non-existent", topicPartition);
            return Collections.emptyList();
        }
        if (partitionEndOffset == 0) {
            LOGGER.info("Topic is empty, skipping consuming");
            return Collections.emptyList();
        }

        List<ConsumerRecord<RecordKey, byte[]>> collectedRecords = new ArrayList<>();
        long offsetConsumed = -1;
        long offsetToReach = partitionEndOffset - 1;
        while (offsetConsumed < offsetToReach) {
            ConsumerRecords<RecordKey, byte[]> records = consumer.poll(pollDuration);
            for (ConsumerRecord<RecordKey, byte[]> record : records) {
                offsetConsumed = record.offset();
                collectedRecords.add(record);
            }
        }
        return collectedRecords;
    }

    private void processRecord(ConsumerRecord<RecordKey, byte[]> record, long now) {
        RecordKey recordKey = record.key();
        if (recordKey == null) {
            return;
        }

        if (recordKey instanceof EncryptionKeyCandidateKey) {
            handleEncryptionCandidateKey(record);
        } else if (recordKey instanceof EncryptionKey) {
            handleEncryptionKey(record);
        } else if (recordKey instanceof SecretBundleKey) {
            handleSecretKey(record, now);
        } else if (recordKey instanceof CompletionMarkKey) {
            handleCompletionMarkKey(record);
        } else {
            LOGGER.error("Encountered record with unknown key {} at offset {}", record.key(), record.offset());
        }
    }

    private void handleEncryptionCandidateKey(ConsumerRecord<RecordKey, byte[]> record) {
        if (record.value() != null) {
            candidates.put((EncryptionKeyCandidateKey) record.key(), record.value());
        } else {
            candidates.remove((EncryptionKeyCandidateKey) record.key());
        }
    }

    private void handleEncryptionKey(ConsumerRecord<RecordKey, byte[]> record) {
        EncryptionKey encryptionKeyRecordKey = (EncryptionKey) record.key();
        if (record.value() != null) {
            encryptionKeyVersions.add(encryptionKeyRecordKey.getVersion());

            lastEncryptionKeyOffset = record.offset();
            lastEncryptionKeyRecordKey = encryptionKeyRecordKey;
            lastEncryptionKeyRecordValue = record.value();
        } else {
            encryptionKeyVersions.remove(encryptionKeyRecordKey.getVersion());

            if (Objects.equals(encryptionKeyRecordKey, lastEncryptionKeyRecordKey)) {
                lastEncryptionKeyOffset = -1;
                lastEncryptionKeyRecordKey = null;
                lastEncryptionKeyRecordValue = null;
            }
        }
    }

    private void handleSecretKey(ConsumerRecord<RecordKey, byte[]> record, long now) {
        SecretBundleKey secretBundleKey = (SecretBundleKey) record.key();
        if (record.value() != null) {
            SecretBundle<byte[]> bundle = new SecretBundle<>(secretBundleKey.getConnector(), secretBundleKey.getId(),
                    record.offset(), record.timestamp(), record.value());
            unencryptedSecrets.addBundle(bundle);
        } else {
            if (record.timestamp() < now - secretDeleteDelayMs) {
                // Remove old secret version, do not keep for delay
                unencryptedSecrets.removeBundle(secretBundleKey.getConnector(), secretBundleKey.getId());
                secretsToDelete.remove(secretBundleKey);
            } else {
                // Add to secrets to be deleted with delay, but mark them completed
                unencryptedSecrets.markBundleDeleted(secretBundleKey.getConnector(), secretBundleKey.getId(), record.offset());
                secretsToDelete.add(secretBundleKey);
            }
        }
    }

    private void handleCompletionMarkKey(ConsumerRecord<RecordKey, byte[]> record) {
        if (record.value() == null) {
            return;
        }
        CompletionMarkKey completionMarkKey = (CompletionMarkKey) record.key();
        unencryptedSecrets.markBundleCompleted(completionMarkKey.getConnector(), completionMarkKey.getId(),
                record.offset());
    }

    private void createTopicEnsureConfig() {
        TopicAdmin.NewTopicBuilder topicBuilder = TopicAdmin.defineTopic(topicPartition.topic())
                .config(topicConfigs)
                .compacted()
                .partitions(1);
        replicationFactor.ifPresent(topicBuilder::replicationFactor);
        NewTopic topicConfig = topicBuilder.build();

        TopicAdmin admin = null;
        //noinspection TryFinallyCanBeTryWithResources
        try {
            admin = createAdmin();
            TopicAdmin finalAdmin = admin;
            KafkaClientAction.executeWrapped(
                    () -> {
                        createTopicAndCheckConfig(finalAdmin, topicConfig);
                        return null;
                    },
                    "creating secret storage topic",
                    t -> new SecretStorageException("Failed to create secret storage topic", t)
            );
        } finally {
            if (admin != null) {
                admin.close(Duration.ZERO);
            }
        }
    }

    private void createTopicAndCheckConfig(TopicAdmin admin, NewTopic topicConfig) throws InterruptedException {
        Set<String> newTopics = admin.createTopicsWithRetry(topicConfig,
                adminTimeoutMs,
                topicCreateBackoffMs,
                Time.SYSTEM
        );
        if (!newTopics.contains(topicPartition.topic())) {
            // It already exists, so check that the topic cleanup policy is compact only and not delete
            LOGGER.info("Using admin client to check cleanup.policy of '{}' topic is '{}'",
                    topicPartition.topic(), TopicConfig.CLEANUP_POLICY_COMPACT);
            admin.verifyTopicCleanupPolicyOnlyCompact(topicPartition.topic(),
                    KafkaSecretStorageConfig.SECRET_STORAGE_TOPIC_CONFIG, "connector secrets");
        }
        checkConfigWithRetry(admin);
    }

    private void checkConfigWithRetry(TopicAdmin admin) throws InterruptedException {
        int tries = Math.max(1, topicCreateRetries + 1);
        for (int i = 0; i < tries; ++i) {
            if (i != 0) {
                // On retry, do backoff
                Thread.sleep(topicCreateBackoffMs);
            }
            if (isTopicCreated(admin)) {
                return;
            }
        }
        throw new SecretStorageException("Could not check the configuration of the topic in the configured number of retries");
    }

    private boolean isTopicCreated(TopicAdmin admin) {
        TopicDescription topicDescription = admin
                .describeTopics(topicPartition.topic())
                .get(topicPartition.topic());
        if (topicDescription != null) {
            int partitionCount = topicDescription.partitions().size();
            if (1 != partitionCount) {
                LOGGER.error("Topic {} has invalid partition count {}, expected is 1",
                    topicPartition.topic(), partitionCount);
                throw new SecretStorageException("Partition count of topic " + topicPartition.topic()
                    + " must be 1, it is " + partitionCount + " instead");
            }
            return true;
        }
        return false;
    }

    private TopicAdmin createAdmin() {
        return new TopicAdmin(adminConfigs);
    }

    private enum State {
        /**
         * State when the latest encryption key is already found and the cipher was inited.
         */
        FOUND_ENCRYPTION_KEY,
        /**
         * State when an encryption key candidate was promoted to encryption key.
         */
        ENCRYPTION_KEY_CANDIDATE_PROMOTED(FOUND_ENCRYPTION_KEY),
        /**
         * State when an encryption key candidate was produced into the topic.
         */
        CANDIDATE_PRODUCED(ENCRYPTION_KEY_CANDIDATE_PROMOTED, FOUND_ENCRYPTION_KEY),
        /**
         * State when the initialization is about to start.
         */
        UNINITIALIZED(CANDIDATE_PRODUCED, ENCRYPTION_KEY_CANDIDATE_PROMOTED, FOUND_ENCRYPTION_KEY);

        private final Set<State> nextValidStates;

        State(State... nextValidStates) {
            Set<State> states = new HashSet<>(Arrays.asList(nextValidStates));
            this.nextValidStates = Collections.unmodifiableSet(states);
        }

        public boolean isValidNextState(State nextState) {
            return nextValidStates.contains(nextState);
        }
    }
}
