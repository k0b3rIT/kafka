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

import java.util.Objects;

import javax.crypto.SecretKey;

public final class SecretKeyFile implements Comparable<SecretKeyFile> {
    private final SecretKey secretKey;
    private final String fileName;

    public SecretKeyFile(SecretKey secretKey, String fileName) {
        this.secretKey = Objects.requireNonNull(secretKey);
        this.fileName = Objects.requireNonNull(fileName);
    }

    public SecretKey getSecretKey() {
        return secretKey;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public int compareTo(SecretKeyFile o) {
        return fileName.compareTo(o.fileName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecretKeyFile that = (SecretKeyFile) o;
        return secretKey.equals(that.secretKey) && fileName.equals(that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(secretKey, fileName);
    }
}
