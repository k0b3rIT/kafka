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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test implementation using the identity function as encryption/decryption.
 * Capable of saving the applied encryption key into a registry using a configured key to be later examined by tests.
 */
public class IdentityCipher implements SecretCipher {
    public static final String CIPHER_ID = IdentityCipher.class.getName() + ".id";
    public static final String UPDATED_ENCRYPTION_KEY = IdentityCipher.class.getName() + ".updated.key";
    private static final Map<String, byte[]> ENCRYPTION_KEYS = new ConcurrentHashMap<>();

    private String id;
    private byte[] updatedKey;

    public static Map<String, byte[]> getEncryptionKeys() {
        return ENCRYPTION_KEYS;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        id = (String) configs.get(CIPHER_ID);
        updatedKey = (byte[]) configs.get(UPDATED_ENCRYPTION_KEY);
    }

    @Override
    public byte[] generateEncryptionKey() {
        return UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Optional<byte[]> initializeEncryptionKey(byte[] encryptedEncryptionKey) {
        if (id != null) {
            ENCRYPTION_KEYS.put(id, encryptedEncryptionKey);
        }
        if (updatedKey != null) {
            return Optional.of(updatedKey);
        }
        return Optional.empty();
    }

    @Override
    public byte[] encryptWithSignature(byte[] value) {
        return value;
    }

    @Override
    public byte[] decrypt(byte[] encryptedValue) {
        return encryptedValue;
    }

    @Override
    public void encryptionKeySaved(byte[] encryptedEncryptionKey) throws SecretCipherException {

    }
}
