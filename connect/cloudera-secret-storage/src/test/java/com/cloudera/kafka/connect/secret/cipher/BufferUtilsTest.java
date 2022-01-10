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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BufferUtilsTest {
    private final byte[] expectedData = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};

    @Test
    public void testBytesReadAtLimit() {
        ByteBuffer buffer = createBuffer(expectedData.length);

        byte[] actualData = BufferUtils.readSizedArray(buffer, expectedData.length);

        assertArrayEquals(expectedData, actualData);
    }

    @Test
    public void testBytesReadUnderLimit() {
        ByteBuffer buffer = createBuffer(expectedData.length);

        byte[] actualData = BufferUtils.readSizedArray(buffer, expectedData.length + 1);

        assertArrayEquals(expectedData, actualData);
    }

    @Test
    public void testBytesReadOverLimitThrows() {
        ByteBuffer buffer = createBuffer(expectedData.length);

        assertThrows(SecretCipherException.class,
                () -> BufferUtils.readSizedArray(buffer, expectedData.length - 1));
    }

    @Test
    public void testBytesReadInsufficientDataThrows() {
        ByteBuffer buffer = createBuffer(expectedData.length + 1);

        assertThrows(SecretCipherException.class,
                () -> BufferUtils.readSizedArray(buffer, expectedData.length + 1));
    }

    @Test
    public void testBytesReadWithNoSizeThrows() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) 33);
        buffer.flip();

        assertThrows(SecretCipherException.class,
                () -> BufferUtils.readSizedArray(buffer, 10));
    }

    private ByteBuffer createBuffer(int size) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 8);
        buffer.putInt(size);
        buffer.put(expectedData);
        buffer.flip();
        return buffer;
    }
}
