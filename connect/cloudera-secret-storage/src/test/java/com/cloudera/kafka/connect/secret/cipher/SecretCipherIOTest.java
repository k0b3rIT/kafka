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
package com.cloudera.kafka.connect.secret.cipher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecretCipherIOTest {
    private static final SecureRandom RANDOM = new SecureRandom();

    private final byte[] mockKey = new byte[64];
    @TempDir
    Path tempDir;
    private DefaultSecretCipher cipher;

    @BeforeEach
    void setUp() {
        RANDOM.nextBytes(mockKey);
        cipher = new DefaultSecretCipher();
        cipher.configure(buildDefaultConfig());
    }

    private Map<String, Object> buildDefaultConfig() {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(SecretCipherConfig.PBE_SALT_CONFIG, "Password based encryption salt.");
        config.put(SecretCipherConfig.PBE_ITERATION_CONFIG, 300000);
        config.put(SecretCipherConfig.GLOBAL_KEY_LOCATION_CONFIG, tempDir.toAbsolutePath().toString());
        config.put(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, "secret");

        return config;
    }

    @Test
    void persistGlobalKey() throws IOException {
        SecretKey secret = new SecretKeySpec(mockKey, "AES");
        cipher.persistGlobalKeyAndGetFileName(secret, tempDir.toAbsolutePath().toString());

        File[] files = tempDir.toFile().listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);
        assertTrue(Files.isReadable(files[0].toPath()));

        byte[] data = Files.readAllBytes(files[0].toPath());

        // version byte + 4 byte encoded key material length  = 5
        byte[] keyMaterial = Arrays.copyOfRange(data, 5, data.length);
        SecretKey read = new SecretKeySpec(keyMaterial, "AES");

        assertEquals(secret, read);
    }

    @Test
    void readGlobalKeys() {
        Path location2 = tempDir.resolve("2" + SecretCipherHelper.GLOBAL_KEY_FILE_EXTENSION);
        byte[] mockKey2 = new byte[64];
        RANDOM.nextBytes(mockKey2);
        SecretKey secret2 = new SecretKeySpec(mockKey2, "AES");
        writeSecretKey(location2, mockKey2);

        Path location = tempDir.resolve("1" + SecretCipherHelper.GLOBAL_KEY_FILE_EXTENSION);
        SecretKey secret = new SecretKeySpec(mockKey, "AES");
        writeSecretKey(location, mockKey);

        List<SecretKeyFile> expected = new ArrayList<>();
        expected.add(new SecretKeyFile(secret2, location2.getFileName().toString()));
        expected.add(new SecretKeyFile(secret, location.getFileName().toString()));

        List<SecretKeyFile> read = cipher.readGlobalKeys(tempDir.toString());

        assertEquals(expected, read);
    }

    @Test
    void cleanUpPreviousKeys() throws IOException {
        Files.write(tempDir.resolve("1" + SecretCipherHelper.GLOBAL_KEY_FILE_EXTENSION), new byte[]{1});
        Files.write(tempDir.resolve("2" + SecretCipherHelper.GLOBAL_KEY_FILE_EXTENSION), new byte[]{2});
        Files.write(tempDir.resolve("3" + SecretCipherHelper.GLOBAL_KEY_FILE_EXTENSION), new byte[]{3});
        Files.write(tempDir.resolve("4" + SecretCipherHelper.GLOBAL_KEY_FILE_EXTENSION), new byte[]{4});

        Set<String> expectedFileNames = new HashSet<>();
        expectedFileNames.add("3" + SecretCipherHelper.GLOBAL_KEY_FILE_EXTENSION);
        expectedFileNames.add("4" + SecretCipherHelper.GLOBAL_KEY_FILE_EXTENSION);

        SecretCipherHelper.cleanUpKeysBefore(tempDir.toAbsolutePath().toString(), "3" + SecretCipherHelper.GLOBAL_KEY_FILE_EXTENSION);

        File[] files = tempDir.toFile().listFiles();
        assertNotNull(files);
        Set<String> actualFileNames = Arrays.stream(files).map(File::getName).collect(Collectors.toSet());
        assertEquals(expectedFileNames, actualFileNames);
    }

    private void writeSecretKey(Path location, byte[] key) {
        try {
            // version
            Files.write(location, new byte[] {1});
            // length
            Files.write(location, new byte[] {0, 0, 0, 64}, StandardOpenOption.APPEND);
            // key material
            Files.write(location, key, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}