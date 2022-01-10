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


import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;

import com.cloudera.kafka.connect.common.TestConnectRestServer;
import com.cloudera.kafka.connect.common.TestUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.easymock.Capture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConnectSecretValidationFilterIntegrationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String CONFIG_PROVIDER_ALIAS = "secret";

    private TestConnectRestServer restServer;
    private ConnectorsResource connectorResource;
    private ConnectorPluginsResource pluginsResource;
    private SecretStorage mockSecretStorage;

    private static String toJson(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    public void setupSecretValidationFilter() {
        restServer = new TestConnectRestServer();
        mockSecretStorage = mock(SecretStorage.class);
        ConnectSecretValidationFilter secretValidationFilter = new ConnectSecretValidationFilter(mockSecretStorage,
                CONFIG_PROVIDER_ALIAS);
        connectorResource = mock(ConnectorsResource.class);
        pluginsResource = mock(ConnectorPluginsResource.class);
        restServer.start();
        restServer.initializeResources(Arrays.asList(secretValidationFilter, connectorResource, pluginsResource));
    }

    @AfterEach
    public void tearDown() {
        if (restServer != null) {
            restServer.stop();
        }
    }

    @Test
    public void testPostConnectorSuccess() throws Throwable {
        String json = "{\"name\":\"new-connector\", \"config\":{\"topic\":\"test\", " +
                "\"secret.properties\":\"jdbc-password\"," +
                "\"jdbc-password\":\"new_secret\"," +
                "\"file-prop\":\"${file:/tmp/path:property-name}\"}}";
        HttpPost request = TestUtils.createHttpPostRequest(restServer.serverBaseUrl(), "/connectors", json);
        Map<String, String> expectedConfig = new HashMap<>();
        expectedConfig.put("topic", "test");
        expectedConfig.put("secret.properties", "jdbc-password");
        expectedConfig.put("jdbc-password", "new_secret");
        expectedConfig.put("file-prop", "${file:/tmp/path:property-name}");
        Capture<CreateConnectorRequest> requestCapture = newCapture();
        CreateConnectorRequest expectedRequest = new CreateConnectorRequest(
                "new-connector",
                expectedConfig,
                null
        );
        expect(connectorResource.createConnector(anyObject(), anyObject(), capture(requestCapture))).andReturn(Response.ok().build());
        replay(connectorResource);

        HttpResponse response = TestUtils.execute(request);

        assertEquals(200, response.getStatusLine().getStatusCode());
        verify(connectorResource);
        assertEquals(expectedRequest, requestCapture.getValue());
    }

    @Test
    public void testEditConfigSuccess() throws Throwable {
        Map<String, String> expectedConfig = new HashMap<>();
        expectedConfig.put("name", "new-connector");
        expectedConfig.put("topic", "test");
        expectedConfig.put("secret.bundle.id", "bundle_id");
        expectedConfig.put("secret.properties", "jdbc-password");
        expectedConfig.put("jdbc-password", "new_secret");
        expectedConfig.put("file-prop", "${file:/tmp/path:property-name}");
        String json = toJson(expectedConfig);
        HttpPut request = new HttpPut(TestUtils.createUrl(restServer.serverBaseUrl(), "/connectors/new-connector/config"));
        TestUtils.setRequestEntity(request, json);

        Capture<String> connectorNameCapture = newCapture();
        Capture<Map<String, String>> configCapture = newCapture();
        expect(connectorResource.putConnectorConfig(capture(connectorNameCapture), anyObject(), anyObject(), capture(configCapture)))
                .andReturn(Response.ok().build());
        Map<String, String> secrets = new HashMap<>();
        secrets.put("jdbc-password", "secret");
        expect(mockSecretStorage.getSecrets(anyString(), anyString(), eq(false))).andReturn(secrets);
        replay(connectorResource, mockSecretStorage);

        HttpResponse response = TestUtils.execute(request);

        assertEquals(200, response.getStatusLine().getStatusCode());
        verify(connectorResource, mockSecretStorage);
        assertEquals("new-connector", connectorNameCapture.getValue());
        assertEquals(expectedConfig, configCapture.getValue());
    }

    @Test
    public void testValidateConfigSuccess() throws Throwable {
        Map<String, String> expectedConfig = new HashMap<>();
        expectedConfig.put("name", "new-connector");
        expectedConfig.put("topic", "test");
        expectedConfig.put("secret.bundle.id", "bundle_id");
        expectedConfig.put("secret.properties", "jdbc-password");
        expectedConfig.put("jdbc-password", "new_secret");
        expectedConfig.put("file-prop", "${file:/tmp/path:property-name}");
        String json = toJson(expectedConfig);
        HttpPut request = new HttpPut(TestUtils.createUrl(restServer.serverBaseUrl(),
                "/connector-plugins/type/config/validate"));
        TestUtils.setRequestEntity(request, json);
        Capture<Map<String, String>> configCapture = newCapture();
        expect(pluginsResource.validateConfigs(anyString(), capture(configCapture)))
                .andReturn(new ConfigInfos(null, 0, null, null));
        Map<String, String> secrets = new HashMap<>();
        secrets.put("jdbc-password", "secret");
        expect(mockSecretStorage.getSecrets(anyString(), anyString(), eq(false))).andReturn(secrets);
        replay(pluginsResource, mockSecretStorage);

        HttpResponse response = TestUtils.execute(request);

        assertEquals(200, response.getStatusLine().getStatusCode());
        verify(pluginsResource, mockSecretStorage);
        assertEquals(expectedConfig, configCapture.getValue());
    }

    @Test
    public void testCreateRequestParseFailure() throws Throwable {
        Map<String, Object> expectedConfig = new HashMap<>();
        Map<String, String> connectorConfig = new HashMap<>();
        expectedConfig.put("name", "my-connector");
        connectorConfig.put("name", "not-my-connector");
        expectedConfig.put("config", connectorConfig);
        String json = toJson(expectedConfig);
        HttpPost request = new HttpPost(TestUtils.createUrl(restServer.serverBaseUrl(),
                "/connectors"));
        TestUtils.setRequestEntity(request, json);
        replay(connectorResource);

        HttpResponse response = TestUtils.execute(request);

        assertEquals(BAD_REQUEST.getStatusCode(), response.getStatusLine().getStatusCode());
        verify(connectorResource);
    }


}
