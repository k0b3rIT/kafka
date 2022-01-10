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

import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

abstract class PackerSelector<T> {

    /**
     * A "registry" describes the conditions of using a certain kind of serialization strategy.
     * Implementors of {@link ByteArraySerializable} are representing _how_ a certain T can be
     * serialized to a byte array and deserialized from it.
     *
     * The various implementations (subclasses of {@link AbstractPacker<T>})
     * of these serde algorithms are differentiated with a version number and a predicate. These two
     * are applied on different objects: version number is determined from the serialized
     * form of an object, just by looking up the version tag. The predicate, on the
     * other hand, applied onto the object to be serialized, to determine whether a certain object  is
     * a subject of a certain type of serialization method.
     *
     * These two combined with the chosen type of serde, is the "registry" returned by {@link #getRegistry()}
     * 
     * Out of this table, two lookup-maps are constructed {@link #packerMap} and {@link #readerMap}, and
     * used by generic search methods {@link #selectVersionedPacker(Object)} and {@link #selectVersionedReader(byte[])}
     */
    
    private Map<Predicate<T>, Supplier<ByteArraySerializable<T>>> packerMap;
    private Map<Predicate<ByteBuffer>, Supplier<ByteArraySerializable<T>>> readerMap;

    protected void initMaps() {
        packerMap = getRegistry()
                .entrySet()
                .stream()
                .sorted(Comparator.comparing(o -> o.getValue().getLeft()))
                .collect(Collectors.toMap(
                        e -> e.getValue().getRight(),
                        e -> e.getKey(),
                        (v1, v2) -> v2,
                        LinkedHashMap::new
                ));

        readerMap = getRegistry()
                .entrySet()
                .stream()
                .sorted(Comparator.comparing(o -> o.getValue().getLeft()))
                .collect(Collectors.toMap(
                        e -> (ByteBuffer buf) -> buf.get(0) == e.getValue().getLeft(),
                        e -> e.getKey(),
                        (v1, v2) -> v2,
                        LinkedHashMap::new
                ));
    }

    protected abstract Map<Supplier<ByteArraySerializable<T>>, Pair<Integer, Predicate<T>>> getRegistry();

    protected ByteArraySerializable<T> selectVersionedPacker(T item) {
        List<Supplier<ByteArraySerializable<T>>> suppliers = packerMap
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().test(item))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

        if (suppliers.isEmpty()) {
            throw new SecretCipherException("Item can't be represented with a byte array");
        }

        // select the latest version
        ByteArraySerializable<T> versionedPacker = suppliers.get(suppliers.size() - 1).get();
        versionedPacker.init(item);

        return versionedPacker;
    }

    protected ByteArraySerializable<T> selectVersionedReader(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        Optional<Supplier<ByteArraySerializable<T>>> optionalSupplier = readerMap
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().test(buffer))
                .map(Map.Entry::getValue)
                .findFirst();

        if (!optionalSupplier.isPresent()) {
            throw new SecretCipherException("Item can't be unambiguously reconstructed from a byte array");
        }

        // there must be one in the collection, see above test, and stream findFirst
        ByteArraySerializable<T> versionedPacker = optionalSupplier.get().get();
        versionedPacker.init(buffer);

        return versionedPacker;
    }


}
