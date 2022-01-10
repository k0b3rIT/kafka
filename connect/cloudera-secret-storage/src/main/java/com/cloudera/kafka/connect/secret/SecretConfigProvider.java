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

import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.provider.ConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Responsible for providing configurations read from a {@link SecretStorage}.
 * The storage instance is fetched lazily from {@link SecretStorageSingleton}.
 */
public class SecretConfigProvider implements ConfigProvider {
    private static final Logger log = LoggerFactory.getLogger(SecretConfigProvider.class);

    private SecretStorage secretStorage;

    public SecretConfigProvider() {
    }

    // Visible for testing
    SecretConfigProvider(SecretStorage secretStorage) {
        this();
        this.secretStorage = secretStorage;
    }

    /**
     * Returns all the secret properties that belong to the connector
     *
     * @param path the name of the connector and version separated by "/"
     * @return all the secrets of the connector
     */
    @Override
    public ConfigData get(String path) {
        return new ConfigData(getSecrets(path));
    }

    @Override
    public ConfigData get(String path, Set<String> keys) {
        Map<String, String> secrets = getSecrets(path);
        secrets.keySet().retainAll(keys);
        return new ConfigData(secrets);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        ConnectSecretStorageConfig config = new ConnectSecretStorageConfig(configs);
        secretStorage = createStorage(config);
    }

    //Visible for testing
    SecretStorage createStorage(ConnectSecretStorageConfig config) {
        return SecretStorageSingleton.getOrCreate(config);
    }

    private Map<String, String> getSecrets(String path) {
        BundlePath bundlePath = BundlePath.parseFromPath(path);
        if (bundlePath == null) {
            log.warn("Request with invalid path: {}", path);
            return Collections.emptyMap();
        }
        return getSecrets(bundlePath.getConnector(), bundlePath.getBundleId());
    }

    private Map<String, String> getSecrets(String connector, String id) {
        log.debug("Retrieve secrets for connector {} with id {}.", connector, id);
        Map<String, String> secrets = secretStorage.getSecrets(connector, id, true);
        if (secrets == null) {
            return new HashMap<>();
        } else {
            return new HashMap<>(secrets);
        }
    }
}
