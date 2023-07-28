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
package com.cloudera.kafka.connect.rest.authorization.extension;


import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthSecurityRestExtension;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;
import org.apache.kafka.connect.tools.MockSinkConnector;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import com.cloudera.kafka.connect.authorization.impl.RoleAuthorizer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import javax.security.auth.login.Configuration;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class AuthorizationRestExtensionIntegrationTest {

    private static EmbeddedConnectCluster connectCluster;
    private static Configuration priorConfiguration;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String PLAIN_LOGIN_MODULE = PlainLoginModule.class.getName();
    private static final String AUTHORIZER = RoleAuthorizer.class.getName();
    private static final String REST_EXTENSIONS = BasicAuthSecurityRestExtension.class.getName() + "," + AuthorizationSecurityRestExtension.class.getName();

    private static final String ADMIN_USER = "testAdmin";
    private static final String OPERATOR_USER = "testOperator";
    private static final String VIEWER_USER = "testViewer";
    private static final String SUPER_USER = "testSuper";

    private static final String TEST_CONNECTOR_CLASS = MockSinkConnector.class.getName();

    @BeforeAll
    public static void setup() {
        Map<String, String> workerProps = new HashMap<>();

        // Register Authorizer and Roles
        workerProps.put("kafka.connect.authorizer.class.name", AUTHORIZER);
        workerProps.put("kafka.connect.authorizer.role.admin.users", ADMIN_USER);
        workerProps.put("kafka.connect.authorizer.role.operator.users", OPERATOR_USER);
        workerProps.put("kafka.connect.authorizer.role.viewer.users", VIEWER_USER);
        workerProps.put("kafka.connect.authorizer.super.user.principal.names", SUPER_USER);

        // Register rest extension
        workerProps.put(RestServerConfig.REST_EXTENSION_CLASSES_CONFIG, REST_EXTENSIONS);

        // Save prior configuration
        priorConfiguration = Configuration.getConfiguration();

        // Setup JAAS config and overwrite Configuration
        Configuration jaasConfig = setupPlainJaasConfig("KafkaConnect");
        Configuration.setConfiguration(jaasConfig);

        // Setup connect cluster
        connectCluster = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(1)
                .numBrokers(1)
                .workerProps(workerProps)
                .build();

        //Start the clusters
        connectCluster.start();
    }

    @AfterAll
    public static void teardown() {
        Configuration.setConfiguration(priorConfiguration);
        connectCluster.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = {ADMIN_USER, OPERATOR_USER, VIEWER_USER, SUPER_USER})
    public void testListPluginsRequest(String user) {
        String endpointUrl = connectCluster.endpointForResource("connector-plugins");
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", authHeader("Basic", user, "password"));

        Response response = sendHttpRequest(endpointUrl, null, headers, HttpMethod.GET);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @ParameterizedTest
    @MethodSource("argumentsForCreateRequest")
    public void testCreateRequest(String user, int expectedResponseCode) {
        String connectorName = user + "-mock-create-connector";
        String endpointUrl = connectCluster.endpointForResource("connectors");

        Map<String, Object> connectorConfig = mockConnectorConfig(connectorName);
        String body = toJson(connectorConfig);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", authHeader("Basic", user, "password1"));

        Response response = connectCluster.requestPost(endpointUrl, body, headers);
        assertEquals(expectedResponseCode, response.getStatus());
    }

    @ParameterizedTest
    @MethodSource("argumentsForValidateRequest")
    public void testValidateRequest(String user, int expectedResponseCode) {
        String endpointUrl = connectCluster.endpointForResource(String.format("connector-plugins/%s/config/validate", TEST_CONNECTOR_CLASS));

        Map<String, Object> connectorConfig = mockConnectorConfig("mock-connector");
        String body = toJson(connectorConfig.get("config"));
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", authHeader("Basic", user, "password1"));

        Response response = sendHttpRequest(endpointUrl, body, headers, HttpMethod.PUT);
        assertEquals(expectedResponseCode, response.getStatus());
    }

    @ParameterizedTest
    @MethodSource("argumentsForEditRequest")
    public void tesEditRequest(String user, int expectedResponseCode) {
        String connectorName = "edit-mock-connector";
        String endpointUrl = connectCluster.endpointForResource(String.format("connectors/%s/config", connectorName));

        Map<String, Object> connectorConfig = mockConnectorConfig(connectorName);
        String body = toJson(connectorConfig.get("config"));
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", authHeader("Basic", user, "password1"));

        // Create connector
        createMockConnector(connectorName);
        // Edit connector
        Response editResponse = sendHttpRequest(endpointUrl, body, headers, HttpMethod.PUT);

        assertEquals(expectedResponseCode, editResponse.getStatus());
    }

    @ParameterizedTest
    @MethodSource("argumentsForPauseAndResumeRequest")
    public void testPauseAndResumeRequest(String user, int expectedResponseCode) throws JsonProcessingException {
        String connectorName = user + "-restart-mock-connector";
        String pauseEndpoint = connectCluster.endpointForResource(String.format("connectors/%s/pause", connectorName));
        String resumeEndpoint = connectCluster.endpointForResource(String.format("connectors/%s/resume", connectorName));
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", authHeader("Basic", user, "password1"));

        // Create Mock Connector
        createMockConnector(connectorName);
        // Pause Connector
        Response pauseResponse = sendHttpRequest(pauseEndpoint, null, headers, HttpMethod.PUT);
        // Resume Connector
        Response resumeResponse = sendHttpRequest(resumeEndpoint, null, headers, HttpMethod.PUT);

        assertEquals(expectedResponseCode, pauseResponse.getStatus());
        assertEquals(expectedResponseCode, resumeResponse.getStatus());
    }

    @ParameterizedTest
    @MethodSource("argumentsForLoggerInfoRequest")
    public void testLoggerInfoRequest(String user, int expectedResponseCode) {
        String endpointUrl = connectCluster.endpointForResource("admin/loggers");
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", authHeader("Basic", user, "password1"));

        Response response = sendHttpRequest(endpointUrl, null, headers, HttpMethod.GET);
        assertEquals(expectedResponseCode, response.getStatus());
    }

    private static Stream<Arguments> argumentsForCreateRequest() {
        return Stream.of(
                Arguments.of(ADMIN_USER, Response.Status.CREATED.getStatusCode()),
                Arguments.of(OPERATOR_USER, Response.Status.FORBIDDEN.getStatusCode()),
                Arguments.of(VIEWER_USER, Response.Status.FORBIDDEN.getStatusCode()),
                Arguments.of(SUPER_USER, Response.Status.CREATED.getStatusCode())
        );
    }

    private static Stream<Arguments> argumentsForValidateRequest() {
        return Stream.of(
                Arguments.of(ADMIN_USER, Response.Status.OK.getStatusCode()),
                Arguments.of(OPERATOR_USER, Response.Status.FORBIDDEN.getStatusCode()),
                Arguments.of(VIEWER_USER, Response.Status.FORBIDDEN.getStatusCode()),
                Arguments.of(SUPER_USER, Response.Status.OK.getStatusCode())
        );
    }

    private static Stream<Arguments> argumentsForEditRequest() {
        return Stream.of(
                Arguments.of(ADMIN_USER, Response.Status.OK.getStatusCode()),
                Arguments.of(OPERATOR_USER, Response.Status.FORBIDDEN.getStatusCode()),
                Arguments.of(VIEWER_USER, Response.Status.FORBIDDEN.getStatusCode()),
                Arguments.of(SUPER_USER, Response.Status.OK.getStatusCode())
        );
    }

    private static Stream<Arguments> argumentsForPauseAndResumeRequest() {
        return Stream.of(
                Arguments.of(ADMIN_USER, Response.Status.ACCEPTED.getStatusCode()),
                Arguments.of(OPERATOR_USER, Response.Status.ACCEPTED.getStatusCode()),
                Arguments.of(VIEWER_USER, Response.Status.FORBIDDEN.getStatusCode()),
                Arguments.of(SUPER_USER, Response.Status.ACCEPTED.getStatusCode())
        );
    }

    private static Stream<Arguments> argumentsForLoggerInfoRequest() {
        return Stream.of(
                Arguments.of(ADMIN_USER, Response.Status.OK.getStatusCode()),
                Arguments.of(OPERATOR_USER, Response.Status.NOT_FOUND.getStatusCode()),
                Arguments.of(VIEWER_USER, Response.Status.NOT_FOUND.getStatusCode()),
                Arguments.of(SUPER_USER, Response.Status.OK.getStatusCode())
        );
    }

    private void createMockConnector(String name) {
        String endpointUrl = connectCluster.endpointForResource("connectors");
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", authHeader("Basic", SUPER_USER, "password"));

        Map<String, Object> connectorConfig = mockConnectorConfig(name);
        String body = toJson(connectorConfig);

        connectCluster.requestPost(endpointUrl, body, headers);
    }

    private static Configuration setupPlainJaasConfig(String name) {
        TestJaasConfig configuration = new TestJaasConfig();
        configuration.addEntry(name, PLAIN_LOGIN_MODULE, Collections.emptyMap());
        return configuration;
    }

    private String authHeader(String authorization, String username, String password) {
        return authorization + " " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }

    private Map<String, Object> mockConnectorConfig(String connectorName) {
        Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put("connector.class", TEST_CONNECTOR_CLASS);
        connectorConfig.put("topics", "test-topic");
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", connectorName);
        properties.put("config", connectorConfig);

        return properties;
    }

    private String toJson(Object o) {
        try {
            return OBJECT_MAPPER.writeValueAsString(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Response sendHttpRequest(String url, String body, Map<String, String> headers,
            String httpMethod) {
        try {
            HttpURLConnection httpCon = (HttpURLConnection) new URL(url).openConnection();
            httpCon.setDoOutput(true);
            httpCon.setRequestMethod(httpMethod);
            headers.forEach(httpCon::setRequestProperty);
            if (body != null) {
                httpCon.setRequestProperty("Content-Type", "application/json");
                try (OutputStreamWriter out = new OutputStreamWriter(httpCon.getOutputStream())) {
                    out.write(body);
                }
            }
            try (InputStream is = httpCon.getResponseCode() < HttpURLConnection.HTTP_BAD_REQUEST
                    ? httpCon.getInputStream()
                    : httpCon.getErrorStream()
            ) {
                String responseEntity = responseToString(is);
                return Response.status(Response.Status.fromStatusCode(httpCon.getResponseCode()))
                        .entity(responseEntity)
                        .build();
            }
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    private String responseToString(InputStream stream) throws IOException {
        int c;
        StringBuilder response = new StringBuilder();
        while ((c = stream.read()) != -1) {
            response.append((char) c);
        }
        return response.toString();
    }
}