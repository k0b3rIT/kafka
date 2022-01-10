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

import org.apache.kafka.common.Configurable;

import java.util.Optional;

public interface SecretCipher extends Configurable {

    /**
     * Generate an encryption key for the first time, and encrypt it with the global key.
     * @return The encrypted encryption key.
     * @throws SecretCipherException If the wrapping of encryption key fails
     */
    byte[] generateEncryptionKey() throws SecretCipherException;

    /**
     * Initialize the cipher with the encrypted encryption key. If the encryption key had to be re-encrypted, return
     * the new encrypted encryption key.
     * @param encryptedEncryptionKey The previously saved encryption key encrypted with a global key.
     * @return Non-empty if the encryption key had to be re-encrypted, and should be stored with the new encryption.
     * @throws SecretCipherException If old wrapper key can't be recovered and it is needed, or if unwrapping fails due
     * to wrong wrapper keys
     */
    Optional<byte[]> initializeEncryptionKey(byte[] encryptedEncryptionKey) throws SecretCipherException;

    /**
     * Invoked after {@link #initializeEncryptionKey(byte[])} returned a non-empty result, and the new encryption key
     * was successfully saved.
     * @param encryptedEncryptionKey The encryption key which was saved.
     * @throws SecretCipherException On error.
     */
    void encryptionKeySaved(byte[] encryptedEncryptionKey) throws SecretCipherException;

    /**
     * Encrypts plain text adding tag (GCM Mode)
     * @param value plain text
     * @return cipher text
     * @throws SecretCipherException If cipher configuration is wrong
     */
    byte[] encryptWithSignature(byte[] value) throws SecretCipherException;

    /**
     * Decrypts cipher text, performs integrity check along the way (GCM mode)
     * @param encryptedValue cipher text
     * @return plain text
     * @throws SecretCipherException If cipher configuration is wrong, or integrity check fails
     */
    byte[] decrypt(byte[] encryptedValue) throws SecretCipherException;
}
