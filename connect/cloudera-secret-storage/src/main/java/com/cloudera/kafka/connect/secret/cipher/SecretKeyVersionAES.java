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

import java.nio.ByteBuffer;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

class SecretKeyVersionAES extends AbstractPacker<SecretKey> {

    @Override
    protected byte getVersion() {
        return 1;
    }

    @Override
    public byte[] pack() {
        check(InitializedFrom.ITEM);

        byte[] keyMaterial = item.getEncoded();
        buffer = ByteBuffer.allocate(calculateSize(keyMaterial));

        writeVersion();
        writeSizedArray(keyMaterial);

        return buffer.array();
    }

    @Override
    public SecretKey read() {
        check(InitializedFrom.BUFFER);
        checkVersion();

        byte[] keyMaterial = readSizedArray(buffer.capacity());

        return new SecretKeySpec(keyMaterial, "AES");
    }

}
