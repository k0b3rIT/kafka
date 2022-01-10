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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

public class SecretKeyWrapper {
    private final String keyAlgorithm;
    private final Cipher wrapper;
    private final Cipher unwrapper;

    public SecretKeyWrapper(SecretKey secretKey, String wrapAlgorithm)  {
        // save key algorithm
        keyAlgorithm = secretKey.getAlgorithm();

        try {
            wrapper = Cipher.getInstance(wrapAlgorithm);
            wrapper.init(Cipher.WRAP_MODE, secretKey);

            unwrapper = Cipher.getInstance(wrapAlgorithm);
            unwrapper.init(Cipher.UNWRAP_MODE, secretKey);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
            throw new SecretCipherException("Can't initialize key wrapper.", e);
        }
    }


    byte[] wrapSecretKeyWithCheck(SecretKey unwrapped) {
        try {
            return wrapper.wrap(unwrapped);
        } catch (IllegalBlockSizeException | InvalidKeyException e) {
            throw new SecretCipherException("Can't wrap key", e);
        }
    }

    SecretKey unwrapSecretKeyWithCheck(byte[] encryptedKey) throws InvalidKeyException {
        try {
            return (SecretKey) unwrapper.unwrap(encryptedKey, keyAlgorithm, Cipher.SECRET_KEY);
        } catch (NoSuchAlgorithmException e) {
            throw new SecretCipherException("Can't unwrap key", e);
        }
    }

}
