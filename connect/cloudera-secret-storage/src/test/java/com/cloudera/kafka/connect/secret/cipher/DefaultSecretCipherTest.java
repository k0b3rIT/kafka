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

import com.cloudera.kafka.connect.secret.SecretCipher;
import com.cloudera.kafka.connect.secret.SecretCipherException;

import org.easymock.Capture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

class DefaultSecretCipherTest {
    private static final String FILE_NAME = "1.key";

    private final Capture<SecretKey> capturedKey = Capture.newInstance();
    private DefaultSecretCipher cipher;

    @BeforeEach
    void setUp() {
        cipher = createMockCipher();
    }

    @AfterEach
    void tearDown() {
        verify(cipher);
    }

    private DefaultSecretCipher createMockCipher() {
        return createMockCipher(null);
    }

    private DefaultSecretCipher createMockCipher(List<SecretKeyFile> secretKeyFiles) {
        DefaultSecretCipher cipher = partialMockBuilder(DefaultSecretCipher.class)
                .addMockedMethod("persistGlobalKeyAndGetFileName", SecretKey.class, String.class)
                .addMockedMethod("readGlobalKeys", String.class)
                .addMockedMethod("clearPreviousKeys")
                .createMock();

        expect(cipher.persistGlobalKeyAndGetFileName(capture(capturedKey), anyString())).andReturn(FILE_NAME).times(0, 1);
        expect(cipher.readGlobalKeys(anyString()))
                .andStubAnswer(() -> {
                    if (secretKeyFiles != null) {
                        return secretKeyFiles;
                    }
                    if (capturedKey.hasCaptured()) {
                        return Collections.singletonList(new SecretKeyFile(capturedKey.getValue(), "filename"));
                    } else {
                        return Collections.emptyList();
                    }
                });
        try {
            cipher.clearPreviousKeys();
        } catch (IOException e) {
            throw new IllegalStateException("Should never happen", e);
        }
        expectLastCall().times(0, 1);

        replay(cipher);

        return cipher;
    }

    private static Map<String, Object> createDefaultConfig(Map<String, Object> override) {
        Map<String, Object> config = new HashMap<>();
        config.put(SecretCipherConfig.PBE_SALT_CONFIG, "Omne initium difficile est.");
        config.put(SecretCipherConfig.GLOBAL_KEY_LOCATION_CONFIG, "/tmp");
        config.put(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, "secret");
        config.putAll(override);
        return config;
    }

    @Test
    void generateEncryptionKey() {
        cipher.configure(createDefaultConfig(Collections.emptyMap()));
        byte[] wrappedEncryptionKey = cipher.generateEncryptionKey();

        Assertions.assertNotNull(wrappedEncryptionKey);
    }

    @Test
    void initializeEncryptionKeyHappy() {
        cipher.configure(createDefaultConfig(Collections.emptyMap()));
        byte[] wrappedEncryptionKey = cipher.generateEncryptionKey();
        Optional<byte[]> result = cipher.initializeEncryptionKey(wrappedEncryptionKey);

        Assertions.assertEquals(result, Optional.empty());
    }

    @Test
    void passwordChange() {
        cipher.configure(createDefaultConfig(Collections.emptyMap()));
        byte[] wrappedEncryptionKey = cipher.generateEncryptionKey();
        cipher.initializeEncryptionKey(wrappedEncryptionKey);

        // new cipher with new password
        DefaultSecretCipher newCipher = createMockCipher();
        newCipher.configure(createDefaultConfig(Collections.singletonMap(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, "secret2")));

        Optional<byte[]> result = newCipher.initializeEncryptionKey(wrappedEncryptionKey);

        Assertions.assertNotEquals(result, Optional.empty());
    }

    @Test
    void passwordChangeWithMultipleKeys() {
        cipher.configure(createDefaultConfig(Collections.emptyMap()));
        byte[] wrappedEncryptionKey = cipher.generateEncryptionKey();
        cipher.initializeEncryptionKey(wrappedEncryptionKey);

        List<SecretKeyFile> secretKeyFiles = new ArrayList<>();
        secretKeyFiles.add(new SecretKeyFile(capturedKey.getValue(), "1"));
        byte[] mockKeyContent = new byte[32];
        ThreadLocalRandom.current().nextBytes(mockKeyContent);
        SecretKey mockKey = new SecretKeySpec(mockKeyContent, "AES");
        secretKeyFiles.add(new SecretKeyFile(mockKey, "2"));

        // new cipher with new password
        DefaultSecretCipher newCipher = createMockCipher(secretKeyFiles);
        newCipher.configure(createDefaultConfig(Collections.singletonMap(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, "secret2")));

        Optional<byte[]> result = newCipher.initializeEncryptionKey(wrappedEncryptionKey);

        Assertions.assertNotEquals(result, Optional.empty());
    }

    @Test
    void saltChange() {
        cipher.configure(createDefaultConfig(Collections.emptyMap()));
        byte[] wrappedEncryptionKey = cipher.generateEncryptionKey();
        cipher.initializeEncryptionKey(wrappedEncryptionKey);

        String plainText = "FOOBAR";
        byte[] cipherText = cipher.encryptWithSignature(plainText.getBytes());

        // new cipher attempts to use new salt
        SecretCipher newCipher = createMockCipher();
        newCipher.configure(createDefaultConfig(Collections.singletonMap(SecretCipherConfig.PBE_SALT_CONFIG, "Minden kezdet nehéz.")));
        newCipher.initializeEncryptionKey(wrappedEncryptionKey);

        byte[] resultBytes = newCipher.decrypt(cipherText);
        String result = new String(resultBytes);

        Assertions.assertEquals(plainText, result);
    }


    @Test
    void encryptDecrypt() {
        cipher.configure(createDefaultConfig(Collections.emptyMap()));
        cipher.initializeEncryptionKey(cipher.generateEncryptionKey());

        String plainText = "FOOBAR";
        byte[] cipherText = cipher.encryptWithSignature(plainText.getBytes());
        byte[] resultBytes = cipher.decrypt(cipherText);

        String result = new String(resultBytes);

        Assertions.assertEquals(plainText, result);
    }

    @Test
    void encryptDecryptTwoCiphers() {
        cipher.configure(createDefaultConfig(Collections.emptyMap()));
        byte[] wrappedEncryptionKey = cipher.generateEncryptionKey();
        cipher.initializeEncryptionKey(wrappedEncryptionKey);

        String plainText = "FOOBAR";
        byte[] cipherText = cipher.encryptWithSignature(plainText.getBytes());

        // new cipher (simulates restart)
        SecretCipher newCipher = createMockCipher();
        newCipher.configure(createDefaultConfig(Collections.emptyMap()));
        Optional<byte[]> newKey = newCipher.initializeEncryptionKey(wrappedEncryptionKey);

        // the config is the same, no need for re-wrapping
        Assertions.assertEquals(newKey, Optional.empty());

        // this should succeed
        byte[] resultBytes = newCipher.decrypt(cipherText);
        String result = new String(resultBytes);

        Assertions.assertEquals(plainText, result);
    }

    @Test
    void encryptDecryptTamper() {
        cipher.configure(createDefaultConfig(Collections.emptyMap()));
        cipher.initializeEncryptionKey(cipher.generateEncryptionKey());
        String plainText = "FOOBAR";

        byte[] cipherText = cipher.encryptWithSignature(plainText.getBytes());

        // tamper with
        CipherTestHelper.tamperWith(cipherText);

        Assertions.assertThrows(SecretCipherException.class, () -> cipher.decrypt(cipherText));
    }

    @Test
    void encryptDecryptPasswordChange() {
        cipher.configure(createDefaultConfig(Collections.emptyMap()));
        byte[] wrappedEncryptionKey = cipher.generateEncryptionKey();
        cipher.initializeEncryptionKey(wrappedEncryptionKey);

        // encrypt secret
        String plainText = "FOOBAR";
        byte[] cipherText = cipher.encryptWithSignature(plainText.getBytes());

        DefaultSecretCipher cipher2 = createMockCipher();
        // reconfigure password
        cipher2.configure(createDefaultConfig(Collections.singletonMap(SecretCipherConfig.GLOBAL_PASSWORD_CONFIG, "secret2")));
        Optional<byte[]> newKey = cipher2.initializeEncryptionKey(wrappedEncryptionKey);

        Assertions.assertNotEquals(newKey, Optional.empty());

        // encryption has not been changed, only the wrapping
        String decrypted = new String(cipher2.decrypt(cipherText));

        Assertions.assertEquals(plainText, decrypted);
    }

    @Test
    void initWithoutGlobalKeyLocation() throws IOException {
        Map<String, Object> config = createDefaultConfig(Collections.emptyMap());
        config.remove(SecretCipherConfig.GLOBAL_KEY_LOCATION_CONFIG);

        reset(cipher);
        expect(cipher.readGlobalKeys(isNull())).andReturn(Collections.emptyList());
        cipher.clearPreviousKeys();
        replay(cipher);

        cipher.configure(config);
        byte[] wrappedEncryptionKey = cipher.generateEncryptionKey();
        cipher.initializeEncryptionKey(wrappedEncryptionKey);
    }
}