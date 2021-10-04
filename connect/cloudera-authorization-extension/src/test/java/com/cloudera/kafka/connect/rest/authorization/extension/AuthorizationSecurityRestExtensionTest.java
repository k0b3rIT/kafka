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
// Copyright (c) 2021 Cloudera, Inc. All rights reserved.

package com.cloudera.kafka.connect.rest.authorization.extension;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Configurable;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthorizationSecurityRestExtensionTest {

    private Map<String, Object> configs;

    @BeforeEach
    public void setup() {
        configs = new HashMap<>();
        configs.put(ConnectSecurityConfig.KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG, null);
    }

    @AfterEach
    public void tearDown() {
        ConnectAuthorizerInstance.reset();
    }

    @Test
    public void testNoAuthorizerConfigured() {
        ConnectRestExtensionContext context = niceMock(ConnectRestExtensionContext.class);
        try (AuthorizationSecurityRestExtension extension = new AuthorizationSecurityRestExtension()) {
            extension.configure(configs);
            extension.register(context);
        }
    }

    @Test
    public void testNoAuthorizerConfiguredWithEmptyString() {
        configs.put(ConnectSecurityConfig.KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG, "");
        try (AuthorizationSecurityRestExtension extension = new AuthorizationSecurityRestExtension()) {
            extension.configure(configs);
            assertTrue(extension.getAuthorizer() instanceof NoopAuthorizer);
        }
    }

    @Test
    public void testNotExistingAuthorizerConfigured() {
        configs.put(ConnectSecurityConfig.KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG, "NotExistingClass");
        try (AuthorizationSecurityRestExtension extension = new AuthorizationSecurityRestExtension()) {
            assertThrows(ConfigException.class, () -> extension.configure(configs));
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testEverythingConfigured() {
        ConnectRestExtensionContext context = mock(ConnectRestExtensionContext.class);
        Configurable configurable = mock(Configurable.class);
        ConnectClusterState clusterState = niceMock(ConnectClusterState.class);
        expect(context.clusterState()).andReturn(clusterState);
        expect(context.configurable()).andReturn(configurable);
        expect(configurable.register(isA(ConnectAuthorizationFilter.class))).andReturn(configurable);
        replay(configurable);
        replay(context);
        configs.put(ConnectSecurityConfig.KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG, "com.cloudera.kafka.connect.rest.authorization.extension.TestConnectAuthorizer");
        configs.put(ConnectSecurityConfig.KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_CONFIG, "superUser");

        try (AuthorizationSecurityRestExtension extension = new AuthorizationSecurityRestExtension()) {
            extension.configure(configs);
            extension.register(context);
        }

        verify(configurable);
        verify(context);
    }

    @Test
    public void testGetVersion() {
        try (AuthorizationSecurityRestExtension extension = new AuthorizationSecurityRestExtension()) {
            assertEquals(AppInfoParser.getVersion(), extension.version());
        }
    }
}
