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
package com.cloudera.kafka.connect.secret;

import org.apache.kafka.common.config.ConfigData;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.strictMock;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SecretConfigProviderTest {
    private static final String TEST_CONNECTOR_NAME = "testConnector";
    private static final String TEST_ID = UUID.randomUUID().toString();
    private static final BundlePath TEST_BUNDLE_PATH = new BundlePath(TEST_CONNECTOR_NAME, TEST_ID);
    private static final String TEST_PATH = TEST_BUNDLE_PATH.toPath();
    public static final String JDBC_PASSWORD = "jdbc_password";
    public static final String DELEGATION_TOKEN = "delegation_token";

    private SecretStorage secretStorage;
    private SecretConfigProvider configProvider;

    @BeforeEach
    public void setup() {
        secretStorage = strictMock(SecretStorage.class);
        configProvider = new SecretConfigProvider(secretStorage);
    }

    @AfterEach
    public void teardown() {
        SecretStorageSingleton.reset();
    }

    @Test
    public void testGetAllSecretsForConnector() {
        expect(secretStorage.getSecrets(TEST_CONNECTOR_NAME, TEST_ID, true)).andReturn(baseSecrets());
        replay(secretStorage);

        ConfigData data = configProvider.get(TEST_PATH);

        assertEquals(baseSecrets(), data.data());
        verify(secretStorage);
    }

    @Test
    public void testGetSomeSecretsForConnector() {
        Set<String> keys = Collections.singleton(JDBC_PASSWORD);
        expect(secretStorage.getSecrets(TEST_CONNECTOR_NAME, TEST_ID, true)).andReturn(baseSecrets());
        replay(secretStorage);

        ConfigData data = configProvider.get(TEST_PATH, keys);

        Map<String, String> expected = baseSecrets();
        expected.keySet().removeIf(key -> !key.equals(JDBC_PASSWORD));
        assertEquals(expected, data.data());
        verify(secretStorage);
    }

    @Test
    public void testNoConnectorSpecified() {
        replay(secretStorage);
        
        ConfigData data = configProvider.get(null);

        assertTrue(data.data().isEmpty());
        verify(secretStorage);
    }

    @Test
    public void testInvalidPathSpecified() {
        replay(secretStorage);

        ConfigData data = configProvider.get(TEST_CONNECTOR_NAME);

        assertTrue(data.data().isEmpty());
        verify(secretStorage);
    }

    private Map<String, String> baseSecrets() {
        Map<String, String> secrets = new HashMap<>();
        secrets.put(JDBC_PASSWORD, "SuperSecret!");
        secrets.put(DELEGATION_TOKEN, "SuperSecret2!");
        return secrets;
    }
}
