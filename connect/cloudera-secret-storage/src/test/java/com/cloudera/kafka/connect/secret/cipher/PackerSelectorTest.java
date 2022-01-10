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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class PackerSelectorTest {

    private static class Item {
        private final int version;

        public Item(int version) {
            this.version = version;
        }

        public int getVersion() {
            return version;
        }
    }

    private static class PackerSelectorForItem extends PackerSelector<Item> {

        PackerSelectorForItem() {
            initMaps();
        }

        @Override
        protected Map<Supplier<ByteArraySerializable<Item>>, Pair<Integer, Predicate<Item>>> getRegistry() {
            Map<Supplier<ByteArraySerializable<Item>>, Pair<Integer, Predicate<Item>>> registry = new HashMap<>();
            registry.put(() -> PackerSelectorTest.supplySerializable(1), new ImmutablePair<>(1, item -> item.getVersion() == 1));
            registry.put(() -> PackerSelectorTest.supplySerializable(5), new ImmutablePair<>(5, item -> item.getVersion() == 5));
            registry.put(() -> PackerSelectorTest.supplySerializable(2), new ImmutablePair<>(2, item -> item.getVersion() == 2));
            registry.put(() -> PackerSelectorTest.supplySerializable(6), new ImmutablePair<>(6, item -> item.getVersion() == 6));
            registry.put(() -> PackerSelectorTest.supplySerializable(7), new ImmutablePair<>(7, item -> item.getVersion() == 6));

            return registry;
        }
    }

    // create version "version" serde
    private static ByteArraySerializable<Item> supplySerializable(int version) {
        AbstractPacker<Item> kp = EasyMock.createMock(AbstractPacker.class);

        EasyMock
                .expect(kp.getVersion())
                .andStubAnswer(() -> (byte) version);

        kp.init(EasyMock.isA(Item.class));
        EasyMock
                .expectLastCall()
                .asStub();

        kp.init(EasyMock.isA(ByteBuffer.class));
        EasyMock
                .expectLastCall()
                .asStub();

        EasyMock.replay(kp);

        return kp;
    }

    private static PackerSelector<Item> packerSelector;

    @BeforeAll
    public static void setUp() {
        packerSelector = new PackerSelectorForItem();
    }

    @Test
    public void testSelectedPacker() {
        // version "1" item
        Item it = new Item(1);

        ByteArraySerializable<Item> serde = packerSelector.selectVersionedPacker(it);
        AbstractPacker<Item> versionedPacker = (AbstractPacker<Item>) serde;

        // we expect a version "1" specialized packer
        Assertions.assertEquals(1, versionedPacker.getVersion());
    }

    @Test
    public void testSelectedReader() {
        // version "2" byte array
        byte[] data = new byte[] {0x02};

        ByteArraySerializable<Item> serde = packerSelector.selectVersionedReader(data);
        AbstractPacker<Item> versionedPacker = (AbstractPacker<Item>) serde;

        // we expect a version "2" specialized reader
        Assertions.assertEquals(2, versionedPacker.getVersion());
    }

    @Test
    public void testUnsupportedPacker() {
        // no packer for this item
        Item it = new Item(3);

        Assertions.assertThrows(
                    SecretCipherException.class,
                    () -> packerSelector.selectVersionedPacker(it),
                "Item can't be represented with a byte array"
        );
    }

    @Test
    public void testUnsupportedReader() {
        // version "3" byte array (unsupported)
        byte[] data = new byte[] {0x03};

        Assertions.assertThrows(
                    SecretCipherException.class,
                    () -> packerSelector.selectVersionedReader(data),
                "Item can't be unambiguously reconstructed from a byte array"
        );
    }

    @Test
    public void testSelectLatestPacker() {
        // version "6" item, multiple packers are defined
        Item it = new Item(6);

        ByteArraySerializable<Item> serde = packerSelector.selectVersionedPacker(it);
        AbstractPacker<Item> versionedPacker = (AbstractPacker<Item>) serde;

        // we expect a version "7" specialized packer
        Assertions.assertEquals(7, versionedPacker.getVersion());
    }

}
