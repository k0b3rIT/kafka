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

import com.cloudera.kafka.connect.secret.SecretCipherException;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

public class TextCiphers {
    private static final int GCM_IV_LENGTH = 12;    // in bytes
    private static final int GCM_TAG_LENGTH = 128;  // in bits

    private final SecretKey encryptionKey;
    private final String encryptionOperation;

    public TextCiphers(SecretKey encryptionKey, String encryptionOperation) {
        this.encryptionKey = encryptionKey;
        this.encryptionOperation = encryptionOperation;
    }

    private byte[] createGcmInitializationVector() {
        byte[] initializationVector = new byte[GCM_IV_LENGTH];
        SecretCipherHelper.RANDOM.nextBytes(initializationVector);
        return initializationVector;
    }

    public byte[] encrypt(byte[] plainText) {
        try {
            byte[] initializationVector = createGcmInitializationVector();
            Cipher encryptor = Cipher.getInstance(encryptionOperation);
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, initializationVector);
            encryptor.init(Cipher.ENCRYPT_MODE, encryptionKey, gcmParameterSpec);

            byte[] cipherText = encryptor.doFinal(plainText);

            CryptPack pack = new CryptPackAES(initializationVector, cipherText);

            return SecretCipherHelper.flattenCryptPack(pack);
        } catch (IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new SecretCipherException("Can't encrypt plaintext", e);
        }
    }

    public byte[] decrypt(byte[] bytes)  {
        try {
            CryptPack pack = SecretCipherHelper.createCryptPack(bytes);
            byte[] initializationVector = pack.getIv();
            byte[] cipherText = pack.getCipherText();

            if (initializationVector.length != GCM_IV_LENGTH) {
                throw new SecretCipherException("Unrecognizable initialization vector.");
            }

            Cipher decryptor = Cipher.getInstance(encryptionOperation);
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, initializationVector);
            decryptor.init(Cipher.DECRYPT_MODE, encryptionKey, gcmParameterSpec);

            return decryptor.doFinal(cipherText);
        } catch (IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new SecretCipherException("Can't decrypt ciphertext", e);
        }
    }

}
