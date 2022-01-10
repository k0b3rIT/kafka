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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.cloudera.kafka.connect.secret.SecretCipher;
import com.cloudera.kafka.connect.secret.cipher.DefaultSecretCipher;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class KafkaSecretStorageConfig extends AbstractConfig {
    private static final String CONFIG_PREFIX = "kafka.connect.secret.storage.";
    public static final String TOPIC_CONFIG_PREFIX = CONFIG_PREFIX + "topic.configs.";
    public static final String PRODUCER_CONFIG_PREFIX = CONFIG_PREFIX + "producer.";
    public static final String CONSUMER_CONFIG_PREFIX = CONFIG_PREFIX + "consumer.";
    public static final String ADMIN_CONFIG_PREFIX = CONFIG_PREFIX + "admin.";

    public static final String SECRET_STORAGE_TOPIC_CONFIG = CONFIG_PREFIX + "topic";
    private static final String SECRET_STORAGE_TOPIC_DOC = "The name of the Kafka topic where connector secrets are stored.";

    public static final String SECRET_STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG = CONFIG_PREFIX + "topic.replication.factor";
    private static final String SECRET_STORAGE_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor of the Kafka topic where connector secrets are stored.";

    public static final String SECRET_STORAGE_TOPIC_CREATE_RETRIES_CONFIG = CONFIG_PREFIX + "topic.create.retries";
    private static final String SECRET_STORAGE_TOPIC_CREATE_RETRIES_DOC = "Defines how many times it should retry to create " +
            "the Kafka topic where connector secrets are stored in case the available brokers are less than required based on the replication factor config.";

    public static final String SECRET_STORAGE_TOPIC_CREATE_RETRY_BACKOFF_MS_CONFIG = CONFIG_PREFIX + "topic.create.retry.backoff.ms";
    private static final String SECRET_STORAGE_TOPIC_CREATE_RETRY_BACKOFF_MS_CONFIG_DOC = "Defines how much time it should wait before retrying to " +
            "create the Kafka topic where connector configurations are stored in case the available brokers are less than required based on the replication " +
            "factor config";

    public static final String SECRET_CIPHER_CLASS_NAME_CONFIG = CONFIG_PREFIX + "cipher.class.name";
    private static final String SECRET_CIPHER_CLASS_NAME_DOC = "The class name of the secret cipher implementation to use to encrypt and decrypt secrets.";
    private static final String SECRET_CIPHER_CLASS_NAME_DEFAULT = DefaultSecretCipher.class.getName();

    public static final String SECRET_OPERATION_TIMEOUT_MS_CONFIG = "secret.operation.timeout.ms";
    private static final String SECRET_OPERATION_TIMEOUT_MS_DOC = "Timeout to use on secret storage operations.";
    private static final long SECRET_OPERATION_TIMEOUT_MS_DEFAULT = 5000L;

    public static final String SECRET_CONSUMER_POLL_DURATION_MS_CONFIG = "secret.consumer.poll.duration.ms";
    private static final String SECRET_CONSUMER_POLL_DURATION_MS_DOC = "Poll duration to use when consuming the secret storage topic.";
    private static final long SECRET_CONSUMER_POLL_DURATION_MS_DEFAULT = 5000L;

    public static final String SECRET_CONSUMER_STARTUP_POLL_DURATION_MS_CONFIG = "secret.consumer.startup.poll.duration.ms";
    private static final String SECRET_CONSUMER_STARTUP_POLL_DURATION_MS_DOC = "Poll duration to use when initializing the secret storage. Typically, startup polls should not take too long to be able to quickly initialize the secret storage.";
    private static final long SECRET_CONSUMER_STARTUP_POLL_DURATION_MS_DEFAULT = 2000L;

    public static final String SECRET_PRODUCER_CLOSE_TIMEOUT_MS_CONFIG = "secret.producer.close.timeout.ms";
    private static final String SECRET_PRODUCER_CLOSE_TIMEOUT_MS_DOC = "The timeout of closing the producer.";
    private static final long SECRET_PRODUCER_CLOSE_TIMEOUT_MS_DEFAULT = 3000L;

    public static final String SECRET_DELETE_DELAY_MS_CONFIG = "secret.delete.delay.ms";
    private static final String SECRET_DELETE_DELAY_MS_DOC = "The delay of removing secrets from memory. This is necessary to tolerate the delay between operations accepted by Connect and the actual change being propagated through the cluster.";
    private static final long SECRET_DELETE_DELAY_MS_DEFAULT = 5 * 60 * 1000L;

    public static final String SECRET_CONCURRENT_MODIFICATION_TIMEOUT_MS_CONFIG = "secret.concurrent.modification.timeout.ms";
    private static final String SECRET_CONCURRENT_MODIFICATION_TIMEOUT_MS_DOC = "The timeout to use when checking for unfinished concurrent modifications.";
    private static final long SECRET_CONCURRENT_MODIFICATION_TIMEOUT_MS_DEFAULT = 2 * 60 * 1000L;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    SECRET_STORAGE_TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    SECRET_STORAGE_TOPIC_DOC
            )
            .define(
                    SECRET_STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG,
                    ConfigDef.Type.SHORT,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    SECRET_STORAGE_TOPIC_REPLICATION_FACTOR_DOC
            )
            .define(
                    SECRET_CONCURRENT_MODIFICATION_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    SECRET_CONCURRENT_MODIFICATION_TIMEOUT_MS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    SECRET_CONCURRENT_MODIFICATION_TIMEOUT_MS_DOC
            )
            .define(
                    SECRET_STORAGE_TOPIC_CREATE_RETRIES_CONFIG,
                    ConfigDef.Type.INT,
                    15,
                    ConfigDef.Importance.LOW,
                    SECRET_STORAGE_TOPIC_CREATE_RETRIES_DOC
            )
            .define(
                    SECRET_STORAGE_TOPIC_CREATE_RETRY_BACKOFF_MS_CONFIG,
                    ConfigDef.Type.INT,
                    10000,
                    ConfigDef.Importance.LOW,
                    SECRET_STORAGE_TOPIC_CREATE_RETRY_BACKOFF_MS_CONFIG_DOC
            )
            .define(
                    SECRET_CIPHER_CLASS_NAME_CONFIG,
                    ConfigDef.Type.CLASS,
                    SECRET_CIPHER_CLASS_NAME_DEFAULT,
                    ConfigDef.Importance.LOW,
                    SECRET_CIPHER_CLASS_NAME_DOC
            )
            .define(
                    SECRET_OPERATION_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    SECRET_OPERATION_TIMEOUT_MS_DEFAULT,
                    ConfigDef.Range.atLeast(0),
                    ConfigDef.Importance.LOW,
                    SECRET_OPERATION_TIMEOUT_MS_DOC
            )
            .define(
                    SECRET_CONSUMER_POLL_DURATION_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    SECRET_CONSUMER_POLL_DURATION_MS_DEFAULT,
                    ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.LOW,
                    SECRET_CONSUMER_POLL_DURATION_MS_DOC
            )
            .define(
                    SECRET_CONSUMER_STARTUP_POLL_DURATION_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    SECRET_CONSUMER_STARTUP_POLL_DURATION_MS_DEFAULT,
                    ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.LOW,
                    SECRET_CONSUMER_STARTUP_POLL_DURATION_MS_DOC
            )
            .define(
                    SECRET_PRODUCER_CLOSE_TIMEOUT_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    SECRET_PRODUCER_CLOSE_TIMEOUT_MS_DEFAULT,
                    ConfigDef.Range.atLeast(0),
                    ConfigDef.Importance.LOW,
                    SECRET_PRODUCER_CLOSE_TIMEOUT_MS_DOC
            )
            .define(
                    SECRET_DELETE_DELAY_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    SECRET_DELETE_DELAY_MS_DEFAULT,
                    ConfigDef.Range.atLeast(0),
                    ConfigDef.Importance.LOW,
                    SECRET_DELETE_DELAY_MS_DOC
            );

    public KafkaSecretStorageConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public String getTopic() {
        return getString(SECRET_STORAGE_TOPIC_CONFIG);
    }

    public Optional<Short> getTopicReplicationFactor() {
        return Optional.ofNullable(getShort(SECRET_STORAGE_TOPIC_REPLICATION_FACTOR_CONFIG));
    }

    public int getTopicCreateRetries() {
        return getInt(SECRET_STORAGE_TOPIC_CREATE_RETRIES_CONFIG);
    }

    public int getTopicCreateRetryBackoffMs() {
        return getInt(SECRET_STORAGE_TOPIC_CREATE_RETRY_BACKOFF_MS_CONFIG);
    }

    public SecretCipher getSecretCipher() {
        return getConfiguredInstance(SECRET_CIPHER_CLASS_NAME_CONFIG, SecretCipher.class);
    }

    public long getSecretOperationTimeoutMs() {
        return getLong(SECRET_OPERATION_TIMEOUT_MS_CONFIG);
    }

    public long getSecretConsumerPollDurationMs() {
        return getLong(SECRET_CONSUMER_POLL_DURATION_MS_CONFIG);
    }

    public long getSecretConsumerStartupPollDurationMs() {
        return getLong(SECRET_CONSUMER_STARTUP_POLL_DURATION_MS_CONFIG);
    }

    public long getSecretProducerCloseTimeoutMs() {
        return getLong(SECRET_PRODUCER_CLOSE_TIMEOUT_MS_CONFIG);
    }

    public long getSecretDeleteDelayMs() {
        return getLong(SECRET_DELETE_DELAY_MS_CONFIG);
    }

    public long getSecretConcurrentModificationTimeoutMs() {
        return getLong(SECRET_CONCURRENT_MODIFICATION_TIMEOUT_MS_CONFIG);
    }

    public Map<String, Object> getTopicConfigs() {
        Map<String, Object> topicProps = new HashMap<>(getConfigsWithPrefixStripped(TOPIC_CONFIG_PREFIX));
        Optional<Short> replicationFactor = getTopicReplicationFactor();
        if (replicationFactor.isPresent() && replicationFactor.get() >= 2) {
            topicProps.putIfAbsent(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(replicationFactor.get() - 1));
        }
        topicProps.putIfAbsent(TopicConfig.SEGMENT_MS_CONFIG, String.valueOf(8 * 60 * 60 * 1000));
        return topicProps;
    }

    public Map<String, Object> getAdminConfigs() {
        Map<String, Object> adminProps = new HashMap<>(originals());
        adminProps.putAll(getConfigsWithPrefixStripped(ADMIN_CONFIG_PREFIX));
        adminProps.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, KafkaSecretStorage.class.getSimpleName());
        return adminProps;
    }

    public Map<String, Object> getConsumerConfigs() {
        Map<String, Object> consumerProps = new HashMap<>(originals());
        consumerProps.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StorageKeyDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.putAll(getConfigsWithPrefixStripped(CONSUMER_CONFIG_PREFIX));
        consumerProps.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, KafkaSecretStorage.class.getSimpleName());
        consumerProps.putIfAbsent(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, ConsumerConfig.DEFAULT_FETCH_MAX_BYTES);
        return consumerProps;
    }

    public Map<String, Object> getProducerConfigs() {
        Map<String, Object> producerProps = new HashMap<>(originals());
        producerProps.putAll(getConfigsWithPrefixStripped(PRODUCER_CONFIG_PREFIX));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StorageKeySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, KafkaSecretStorage.class.getSimpleName());
        producerProps.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.putIfAbsent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        return producerProps;
    }

    private Map<String, Object> getConfigsWithPrefixStripped(String prefix) {
        return originals()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        e -> e.getKey().substring(prefix.length()),
                        Map.Entry::getValue
                ));
    }
}
