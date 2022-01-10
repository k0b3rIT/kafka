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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class CryptPacker extends PackerSelector<CryptPack> {

    private static final Map<Supplier<ByteArraySerializable<CryptPack>>, Pair<Integer, Predicate<CryptPack>>> CLASSMAP =
        new HashMap<Supplier<ByteArraySerializable<CryptPack>>, Pair<Integer, Predicate<CryptPack>>>() {
            {
                put(CryptPackAES::new, new ImmutablePair<>(1, cp -> cp.getType().equals("AES256")));
            }
        };

    private static CryptPacker theInstance = null;

    private static synchronized CryptPacker getInstance() {
        if (theInstance == null) {
            theInstance = new CryptPacker();
        }
        return theInstance;
    }

    private CryptPacker() {
        initMaps();
    }

    public static ByteArraySerializable<CryptPack> getVersionedPacker(CryptPack key) {
        return getInstance().selectVersionedPacker(key);
    }

    public static ByteArraySerializable<CryptPack> getVersionedReader(byte[] data) {
        return getInstance().selectVersionedReader(data);
    }

    @Override
    public Map<Supplier<ByteArraySerializable<CryptPack>>, Pair<Integer, Predicate<CryptPack>>> getRegistry() {
        return CLASSMAP;
    }

}
