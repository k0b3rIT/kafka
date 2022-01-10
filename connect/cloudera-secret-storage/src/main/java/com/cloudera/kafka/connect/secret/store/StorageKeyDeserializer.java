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
package com.cloudera.kafka.connect.secret.store;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StorageKeyDeserializer implements Deserializer<RecordKey> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageKeyDeserializer.class);

    private final ObjectMapper objectMapper;

    public StorageKeyDeserializer() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public RecordKey deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, RecordKey.class);
        } catch (IOException e) {
            LOGGER.error("Failed to deserialize record key from topic {}", topic, e);
            return null;
        }
    }
}
