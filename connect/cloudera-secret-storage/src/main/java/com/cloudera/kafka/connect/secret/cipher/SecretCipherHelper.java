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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.crypto.SecretKey;

public class SecretCipherHelper {
    private static final Logger log = LoggerFactory.getLogger(SecretCipherHelper.class);
    static final String GLOBAL_KEY_FILE_EXTENSION = ".key.bk";

    static final SecureRandom RANDOM = new SecureRandom();

    static String persistNextKey(SecretKey key, String path) throws IOException {
        byte[] packedData = SecretKeyPacker.getVersionedPacker(key).pack();

        String nextFileName = System.currentTimeMillis() + GLOBAL_KEY_FILE_EXTENSION;
        Path where = Paths.get(path, nextFileName);
        Files.write(where, packedData);
        Files.setPosixFilePermissions(where, desiredPermissions());
        log.debug("A global key has been persisted to {}", where);

        return nextFileName;
    }

    static List<SecretKeyFile> readKeysAscending(String path) throws IOException {
        List<SecretKeyFile> result = new ArrayList<>();
        for (File file : listKeyFilesSorted(Paths.get(path))) {
            byte[] data = Files.readAllBytes(file.toPath());
            SecretKey secretKey = SecretKeyPacker.getVersionedReader(data).read();
            result.add(new SecretKeyFile(secretKey, file.getName()));
            log.debug("A key has been retrieved from {}.", file);
        }
        result.sort(Comparator.comparing(SecretKeyFile::getFileName));
        return result;
    }

    static void cleanUpKeysBefore(String path, String keyFileName) throws IOException {
        List<File> files = listKeyFilesSorted(Paths.get(path));
        for (File file : files) {
            if (file.getName().compareTo(keyFileName) >= 0) {
                return;
            }
            Files.delete(file.toPath());
            log.debug("Cleaned up key in file {}", file);
        }
    }

    static byte[] flattenCryptPack(CryptPack pack) {
        return CryptPacker.getVersionedPacker(pack).pack();
    }

    static CryptPack createCryptPack(byte[] flattened) {
        return CryptPacker.getVersionedReader(flattened).read();
    }

    private static List<File> listKeyFilesSorted(Path path) throws IOException {
        File[] keyFiles = path
                .toFile()
                .listFiles((dir, name) -> name.endsWith(GLOBAL_KEY_FILE_EXTENSION));
        if (keyFiles == null) {
            throw new IOException("Failed to list key files");
        }
        return Arrays
                .stream(keyFiles)
                .filter(File::isFile)
                .sorted(Comparator.comparing(File::getName))
                .collect(Collectors.toList());
    }

    private static Set<PosixFilePermission> desiredPermissions() {
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);

        return perms;
    }
}
