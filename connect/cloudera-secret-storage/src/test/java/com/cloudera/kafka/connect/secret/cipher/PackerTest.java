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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

public class PackerTest {

    private static final SecureRandom RANDOM = new SecureRandom();

    // toy Item to serialize/deserialize
    private static class Item {
        byte[] array;
        String str;
        Integer cnt;

        public Item(byte[] array, String str, Integer cnt) {
            this.array = array;
            this.str = str;
            this.cnt = cnt;
        }

        public byte[] getArray() {
            return array;
        }

        public String getStr() {
            return str;
        }

        public Integer getCnt() {
            return cnt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item = (Item) o;
            return Arrays.equals(array, item.array) && str.equals(item.str) && cnt.equals(item.cnt);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(str, cnt);
            result = 31 * result + Arrays.hashCode(array);
            return result;
        }
    }

    // serde for Item
    private static class ItemPacker extends AbstractPacker<Item> {

        @Override
        protected byte getVersion() {
            return 1;
        }

        @Override
        public byte[] pack() {
            check(InitializedFrom.ITEM);

            byte[] itemArray = item.getArray();
            byte[] itemString = item.getStr().getBytes(StandardCharsets.UTF_8);
            buffer = ByteBuffer.allocate(calculateSize(itemArray, itemString) + 4); // Integer saved on 4 bytes

            writeVersion();
            writeSizedArray(itemArray);
            writeSizedArray(itemString);
            buffer.putInt(item.getCnt());

            return buffer.array();
        }

        @Override
        public Item read() {
            check(InitializedFrom.BUFFER);
            checkVersion();

            byte[] array = readSizedArray(buffer.capacity());
            byte[] stringBytes = readSizedArray(buffer.capacity());
            int readCnt = buffer.getInt();

            return new Item(array, new String(stringBytes, StandardCharsets.UTF_8), readCnt);
        }

    }

    private Item createRandomIzedItem() {
        byte[] array = new byte[RANDOM.nextInt(100)];
        RANDOM.nextBytes(array);
        return new Item(array, "Foobar", RANDOM.nextInt(1000000));
    }

    @Test
    public void testItemPacker() {
        Item toBeSerialized = createRandomIzedItem();

        ByteArraySerializable<Item> serializer = new ItemPacker();
        serializer.init(toBeSerialized);
        byte[] serialized = serializer.pack();

        ByteArraySerializable<Item> deserializer = new ItemPacker();
        deserializer.init(ByteBuffer.wrap(serialized));
        Item deserialized = deserializer.read();

        Assertions.assertEquals(toBeSerialized, deserialized);
    }

    @Test
    public void testIncorrectVersion() {
        // it is version 2!
        byte[] data = new byte[] {0x02};

        ByteArraySerializable<Item> deserializer = new ItemPacker();
        deserializer.init(ByteBuffer.wrap(data));

        Assertions.assertThrows(
                SecretCipherException.class,
                deserializer::read,
                "Incompatible serializer version 2 is used."
        );
    }


}
