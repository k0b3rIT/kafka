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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SecretKeyWrapperTest {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static SecretKeyWrapper wrapper;

    @BeforeAll
    static void setUp() throws NoSuchAlgorithmException {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256, RANDOM);
        SecretKey secretKey = generator.generateKey();
        wrapper = new SecretKeyWrapper(secretKey, "AESWrap");
    }


    @Test
    void wrapAndUnwrap() throws InvalidKeyException {
        byte[] keyMaterial = new byte[32];
        RANDOM.nextBytes(keyMaterial);
        SecretKey encrypting = new SecretKeySpec(keyMaterial, "AES");

        // wrap
        byte[] wrapped = wrapper.wrapSecretKeyWithCheck(encrypting);

        //unwrap
        SecretKey unwrapped = wrapper.unwrapSecretKeyWithCheck(wrapped);

        assertEquals(encrypting, unwrapped);
    }

    @Test
    void detectCompromisedKey() {
        byte[] keyMaterial = new byte[32];
        RANDOM.nextBytes(keyMaterial);
        SecretKey encrypting = new SecretKeySpec(keyMaterial, "AES");

        // wrap
        byte[] wrapped = wrapper.wrapSecretKeyWithCheck(encrypting);

        // tamper with
        CipherTestHelper.tamperWith(wrapped);

        // detect compromised key -> exception is thrown
        assertThrows(InvalidKeyException.class, () -> {
            wrapper.unwrapSecretKeyWithCheck(wrapped);
        });

    }
}