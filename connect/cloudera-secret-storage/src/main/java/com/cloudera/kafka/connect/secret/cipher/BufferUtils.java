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
// Copyright (c) 2023 Cloudera, Inc. All rights reserved.
package com.cloudera.kafka.connect.secret.cipher;

import com.cloudera.kafka.connect.secret.SecretCipherException;

import java.nio.ByteBuffer;

public class BufferUtils {
    private BufferUtils() {}

    /**
     * Reads a blob from the buffer by first reading an int SIZE, then reading SIZE number of bytes.
     * The maximum allowed size to read can be specified.
     * @param buffer The buffer to read from.
     * @param maxSize The maximum allowed size.
     * @return The blob read from buffer.
     * @throws SecretCipherException If the buffer does not contain the expected amount of data, or the size specified in the buffer exceeds maxSize.
     */
    public static byte[] readSizedArray(ByteBuffer buffer, int maxSize) throws SecretCipherException {
        if (buffer.remaining() < Integer.BYTES) {
            throw new SecretCipherException("Cannot read data size.");
        }
        int size = buffer.getInt();
        if (size > maxSize || size > buffer.remaining()) {
            // prevent buffer size attack (attacker sets array size to Integer.MAX_VALUE)
            throw new SecretCipherException("Unexpected array size of persisted data.");
        }
        byte[] data = new byte[size];
        buffer.get(data);
        return data;
    }
}
