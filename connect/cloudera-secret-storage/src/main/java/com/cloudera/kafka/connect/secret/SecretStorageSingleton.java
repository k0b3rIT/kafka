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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton holder class for a {@link SecretStorage} instance.
 * Implements double-checked locking to minimize synchronization:
 * <a href="https://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java">https://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java</a>
 */
public class SecretStorageSingleton {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecretStorageSingleton.class);

    private static volatile SecretStorage storage;

    public static SecretStorage getOrCreate(ConnectSecretStorageConfig config) {
        SecretStorage localStorage = storage;
        if (localStorage == null) {
            synchronized (SecretStorageSingleton.class) {
                localStorage = storage;
                if (localStorage == null) {
                    if (config == null) {
                        return null;
                    }
                    storage = localStorage = createStorage(config);
                }
            }
        }
        return localStorage;
    }

    // Visible for testing
    public static void reset() {
        synchronized (SecretStorageSingleton.class) {
            storage = null;
        }
    }

    private static SecretStorage createStorage(ConnectSecretStorageConfig config) {
        try {
            return config.getSecretStorage();
        } catch (Exception e) {
            LOGGER.error("Failed to create SecretStorage instance", e);
            throw new RuntimeException(e);
        }
    }
}
