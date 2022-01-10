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
import java.util.Arrays;

abstract class AbstractPacker<T> implements ByteArraySerializable<T> {
    enum InitializedFrom { BUFFER, ITEM }

    protected InitializedFrom origin;
    protected ByteBuffer buffer;
    protected T item;

    @Override
    public void init(T t) {
        origin = InitializedFrom.ITEM;
        this.item = t;
    }

    @Override
    public void init(ByteBuffer buffer) {
        origin = InitializedFrom.BUFFER;
        this.buffer = buffer;
    }

    protected abstract byte getVersion();

    protected void writeVersion() {
        buffer.put(getVersion());
    }

    protected void checkVersion() {
        byte version = buffer.get();
        if (version != getVersion()) {
            throw new SecretCipherException(String.format("Incompatible serializer version %d is used.", version));
        }
    }

    protected void writeSizedArray(byte[] data) {
        buffer.putInt(data.length);
        buffer.put(data);
    }

    protected byte[] readSizedArray(int maxSize) {
        return BufferUtils.readSizedArray(buffer, maxSize);
    }

    protected static int calculateSize(byte[]... payloads) {
        return Arrays.stream(payloads)
                .mapToInt(bytes -> Integer.BYTES + bytes.length)
                .sum() + 1;
    }

    protected void check(InitializedFrom in) {
        if (origin != in) {
            throw new SecretCipherException("Serializer is incorrectly initialized.");
        }
    }

}
