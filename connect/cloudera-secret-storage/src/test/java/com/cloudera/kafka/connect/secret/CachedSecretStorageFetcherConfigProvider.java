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

import org.apache.kafka.common.KafkaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Instead of using {@link SecretStorageSingleton}, fetches the secret storage instance from {@link SecretStorageCachingExtension}.
 * To be used with {@link SecretStorageCachingExtension}.
 */
public class CachedSecretStorageFetcherConfigProvider extends SecretConfigProvider {
    private static final List<SecretStorage> SECRET_STORAGES = new ArrayList<>();
    private static int currentIndex = 0;

    public static synchronized void reset() {
        SECRET_STORAGES.clear();
        currentIndex = 0;
    }

    public static synchronized void closeSecretStorages() {
        SECRET_STORAGES.forEach(secretStorage -> {
            try {
                secretStorage.close();
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        });
    }

    public static synchronized SecretStorage get() {
        if (SECRET_STORAGES.isEmpty()) {
            throw new IllegalStateException();
        }
        return SECRET_STORAGES.get((currentIndex++) % SECRET_STORAGES.size());
    }

    private static synchronized void add(SecretStorage secretStorage) {
        SECRET_STORAGES.add(secretStorage);
    }

    @Override
    SecretStorage createStorage(ConnectSecretStorageConfig config) {
        SecretStorage storage = config.getSecretStorage();
        add(storage);
        return storage;
    }
}
