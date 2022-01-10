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

import org.apache.kafka.common.config.types.Password;

import com.cloudera.kafka.connect.secret.SecretCipher;
import com.cloudera.kafka.connect.secret.SecretCipherException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public class DefaultSecretCipher implements SecretCipher {

    private static final Logger log = LoggerFactory.getLogger(SecretCipher.class);

    // wrapper constants
    private static final String PBE_ALGORITHM = "PBKDF2WithHmacSHA256";
    private static final String WRAPPING_ALGORITHM = "AESWrap";
    private static final String WRAPPING_ALGORITHM_KEY_TYPE = "AES";
    private static final int WRAPPING_ALGORITHM_KEY_SIZE = 256;

    // cipher constants
    private static final String ENCRYPTION_ALGORITHM = "AES";
    private static final int ENCRYPTION_ALGORITHM_KEY_SIZE = 256;
    private static final String ENCRYPTION_OPERATION = "AES/GCM/NoPadding";


    // config determined fields
    private String globalKeyLocation;
    private Password globalPassword;
    private int iterations;
    private byte[] salt;

    private KeyGenerator keyGenerator;
    private SecretKey secretEncryptionKey;
    private SecretKeyWrapper wrapper;
    private TextCiphers ciphers;

    private SecretKey globalSecretKey;
    private String globalSecretKeyFileName;
    private List<SecretKeyFile> previousGlobalSecretKeys;

    /**
     * Create config object from raw configs given in a map
     * @param configs - raw configs (Kafkaesque)
     */
    @Override
    public void configure(Map<String, ?> configs) {
        initFromConfig(new SecretCipherConfig(configs));
        prepareForUsage();
    }

    private void initFromConfig(SecretCipherConfig config) {
        globalKeyLocation = config.getGlobalKeyLocation();
        globalPassword = config.getGlobalPassword();

        salt = config.getPbeSalt();
        iterations = config.getPbeIterations();
    }

    private void prepareForUsage() {
        try {
            prepareGlobalKey();
            previousGlobalSecretKeys = readGlobalKeys(globalKeyLocation);
            prepareEncryption();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new SecretCipherException("SecretCipher could not be configured.", e);
        }
    }

    /**
     * Derives secret key usable for wrapping from a password
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeySpecException
     */
    private void prepareGlobalKey() throws
            NoSuchAlgorithmException,
            InvalidKeySpecException {

        // derive secret key from global password
        SecretKeyFactory pbeKeyFactory = SecretKeyFactory.getInstance(PBE_ALGORITHM);
        PBEKeySpec pbeKeySpec = new PBEKeySpec(globalPassword.value().toCharArray(), salt, iterations, WRAPPING_ALGORITHM_KEY_SIZE);
        globalSecretKey = new SecretKeySpec(pbeKeyFactory.generateSecret(pbeKeySpec).getEncoded(), WRAPPING_ALGORITHM_KEY_TYPE);
        log.trace("Derived global key from provided password.");

        //create wrappers
        wrapper = new SecretKeyWrapper(globalSecretKey, WRAPPING_ALGORITHM);
    }

    String persistGlobalKeyAndGetFileName(SecretKey globalSecretKey, String path) {
        try {
            return SecretCipherHelper.persistNextKey(globalSecretKey, path);
        } catch (IOException e) {
            log.error("Global key could not be persisted to {}", path);
            throw new SecretCipherException("Global key could not be persisted:", e);
        }
    }

    List<SecretKeyFile> readGlobalKeys(String path) {
        if (path == null) {
            return Collections.emptyList();
        }
        try {
            log.debug("Reading persisted global keys from {}", path);
            List<SecretKeyFile> keys = SecretCipherHelper.readKeysAscending(path);
            Optional<SecretKeyFile> mismatchingAlgorithm = keys
                    .stream()
                    .filter(key -> !WRAPPING_ALGORITHM_KEY_TYPE.equals(key.getSecretKey().getAlgorithm()))
                    .findAny();
            if (mismatchingAlgorithm.isPresent()) {
                log.error("Unexpected key algorithm: {}", mismatchingAlgorithm.get().getSecretKey().getAlgorithm());
                throw new SecretCipherException("Unexpected key algorithm");
            }
            keys = new ArrayList<>(keys);
            keys.sort(Comparator.reverseOrder());
            return keys;
        } catch (IOException e) {
            log.warn("Global key could not be read from {}", path);
            return Collections.emptyList();
        }
    }

    private void tryClearPreviousKeys() {
        try {
            clearPreviousKeys();
        } catch (IOException e) {
            log.warn("Failed to clean up previous keys", e);
        }
    }

    void clearPreviousKeys() throws IOException {
        if (globalKeyLocation == null) {
            return;
        }
        log.debug("Cleaning up previous global keys at {}", globalKeyLocation);
        SecretCipherHelper.cleanUpKeysBefore(globalKeyLocation, globalSecretKeyFileName);
    }

    /**
     * Initializes the necessary key generator and factory. Uses configuration determining
     * algorithm and key size.
     * @throws NoSuchAlgorithmException
     */
    private void prepareEncryption() throws NoSuchAlgorithmException {
        keyGenerator = KeyGenerator.getInstance(ENCRYPTION_ALGORITHM);
        keyGenerator.init(ENCRYPTION_ALGORITHM_KEY_SIZE, SecretCipherHelper.RANDOM);
    }

    /**
     * Sets up cipher facility for subsequent encryption or decryption. This means initialization of ciphers,
     * saving encryption key and consulting with config
     * @return Generated encryption key, wrapped.
     */
    @Override
    public byte[] generateEncryptionKey() {
        log.debug("Generating encryption key.");
        return wrapper.wrapSecretKeyWithCheck(keyGenerator.generateKey());
    }

    @Override
    public Optional<byte[]> initializeEncryptionKey(byte[] encryptedEncryptionKey) {
        if (globalKeyLocation != null) {
            globalSecretKeyFileName = persistGlobalKeyAndGetFileName(globalSecretKey, globalKeyLocation);
        }

        try {
            //try to unwrap - happy path
            log.trace("Trying to unwrap encryption key with newest global key");
            initKeyAndCiphers(wrapper.unwrapSecretKeyWithCheck(encryptedEncryptionKey));
            tryClearPreviousKeys();
            return Optional.empty();
        } catch (InvalidKeyException e) {
            // failed, key probably had been wrapped with the previous global key
            log.warn("Encrypted key is probably wrapped with an older global key", e);
        }

        log.trace("Trying to unwrap encryption key with an older, persisted global key");
        return previousGlobalSecretKeys
                .stream()
                .map(prevKey -> tryUnwrappingWithOldGlobalKey(prevKey, encryptedEncryptionKey))
                .filter(Optional::isPresent)
                .findAny()
                .orElseThrow(() -> new SecretCipherException("Neither configured, nor saved global keys are valid."));
    }

    private Optional<byte[]> tryUnwrappingWithOldGlobalKey(SecretKeyFile previousKey, byte[] encryptedEncryptionKey) {
        SecretKeyWrapper oldWrapper = new SecretKeyWrapper(previousKey.getSecretKey(), WRAPPING_ALGORITHM);
        try {
            initKeyAndCiphers(oldWrapper.unwrapSecretKeyWithCheck(encryptedEncryptionKey));
            return Optional.of(wrapper.wrapSecretKeyWithCheck(secretEncryptionKey));
        } catch (InvalidKeyException e) {
            //Ignore, keep looking for other keys
            log.trace("Unwrap unsuccessful, keep looking for other keys");
            return Optional.empty();
        }
    }

    /**
     * Initializes encryption key member and creates linked ciphers using this key.
     * @param encryptionKey encryption key used for subsequent encryption and decryption
     */
    private void initKeyAndCiphers(SecretKey encryptionKey) {
        log.debug("Using supplied, successfully unwrapped encryption key.");
        secretEncryptionKey = encryptionKey;
        ciphers = new TextCiphers(encryptionKey, ENCRYPTION_OPERATION);
    }


    @Override
    public byte[] encryptWithSignature(byte[] plainText)  {
        return ciphers.encrypt(plainText);
    }

    @Override
    public byte[] decrypt(byte[] cipherText) {
        return ciphers.decrypt(cipherText);
    }

    @Override
    public void encryptionKeySaved(byte[] encryptedEncryptionKey) throws SecretCipherException {
        tryClearPreviousKeys();
    }
}
