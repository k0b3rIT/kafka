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

import java.nio.ByteBuffer;

final class CryptPackAES extends AbstractPacker<CryptPack> implements CryptPack {
    private static final int IV_LENGTH = 12;
    private static final String CRYPTPACK_TYPE = "AES256";

    private byte[] iv;
    private byte[] cipherText;

    public CryptPackAES() {}

    public CryptPackAES(byte[] initializationVector, byte[] cipherText) {
        if (initializationVector.length != IV_LENGTH) {
            throw new SecretCipherException(String.format("Initialization vector must be %d bytes length", IV_LENGTH));
        }
        this.iv = initializationVector;
        this.cipherText = cipherText;
        super.init(this);
    }

    /** This is somewhat special:
     * The class both contains the data, and represents the methods of serializing and deserializing them.
     * Protected member {@link AbstractPacker#item} is equivalent with {@code this}. By calling {@link AbstractPacker#init}
     * we make them equal.
     */
    @Override
    public void init(CryptPack cryptPack) {
        this.iv = cryptPack.getIv();
        this.cipherText = cryptPack.getCipherText();
        super.init(this);
    }

    @Override
    public String getType() {
        return CRYPTPACK_TYPE;
    }

    @Override
    public byte[] getIv() {
        return iv;
    }

    @Override
    public byte[] getCipherText() {
        return cipherText;
    }

    @Override
    protected byte getVersion() {
        return 1;
    }

    @Override
    public CryptPack read() {
        check(InitializedFrom.BUFFER);
        checkVersion();

        iv = readSizedArray(IV_LENGTH);
        cipherText = readSizedArray(buffer.capacity());

        return this;
    }

    @Override
    public byte[] pack() {
        check(InitializedFrom.ITEM);

        buffer = ByteBuffer.allocate(calculateSize(iv, cipherText));
        writeVersion();
        writeSizedArray(iv);
        writeSizedArray(cipherText);

        return buffer.array();
    }
}
