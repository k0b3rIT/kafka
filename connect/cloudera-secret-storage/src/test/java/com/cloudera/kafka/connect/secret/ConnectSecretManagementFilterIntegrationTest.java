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

import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;

import com.cloudera.kafka.connect.common.TestConnectRestServer;
import com.cloudera.kafka.connect.common.TestUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectSecretManagementFilterIntegrationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PROVIDER_ALIAS = "secret";
    private static final String SECRET_PROP_1 = "secret.prop.1";
    private static final String SECRET_PROP_2 = "secret.prop.2";
    private static final String SECRET_PROP_3 = "secret.prop.3";
    private static final Set<String> SECRET_PROPERTIES =
            Stream.of(SECRET_PROP_1, SECRET_PROP_2, SECRET_PROP_3).collect(Collectors.toSet());
    private static final String UUID_1 = "UUID_1";
    private static final String UUID_2 = "UUID_2";

    private static TestConnectRestServer restServer;
    private static SecretStorage secretStorage;
    private static ConnectClusterState clusterState;
    private static ConnectorsResource resource;

    @BeforeAll
    static void setup() {
        restServer = new TestConnectRestServer();
        secretStorage = mock(SecretStorage.class);
        clusterState = mock(ConnectClusterState.class);
        resource = mock(ConnectorsResource.class);
        restServer.initializeResources(Arrays.asList(
                new ConnectSecretManagementFilter(secretStorage, clusterState, PROVIDER_ALIAS),
                resource
        ));
        restServer.start();
    }

    @AfterAll
    static void stopRestServer() {
        if (restServer != null) {
            restServer.stop();
        }
    }

    private static String getSecretReference(String connectorName, String uuid, String propertyKey) {
        return "${" + PROVIDER_ALIAS + ":" + connectorName + "/" + uuid + ":" + propertyKey + "}";
    }

    private static <T> T assertResponseStatusAndGetEntity(int expectedStatus, HttpResponse response, Class<T> entityType) {
        T entity;
        try {
            entity = OBJECT_MAPPER.readValue(response.getEntity().getContent(), entityType);
            String errorMsg = "";
            if (entity instanceof ErrorMessage) {
                errorMsg = ((ErrorMessage) entity).message();
            }
            assertEquals(expectedStatus, response.getStatusLine().getStatusCode(), errorMsg);
        } catch (Exception e) {
            return null;
        }
        return entity;
    }

    private Map<String, String> getCreateRequestConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector");
        String secretProperties = String.join(",", SECRET_PROPERTIES);
        config.put(SecretStorageExtension.SENSITIVE_PROPERTY_LIST, secretProperties);
        config.put("non_secret_config", "non_secret_value");
        config.putAll(plainSecrets());
        return config;
    }

    private Map<String, String> getFilteredCreateRequestConfig(String connectorName, String uuid) {
        Map<String, String> config = new HashMap<>(getCreateRequestConfig());
        SECRET_PROPERTIES.forEach(x -> config.compute(x, (key, value) -> getSecretReference(connectorName, uuid, key)));
        config.put(SecretStorageExtension.SECRET_BUNDLE_ID, uuid);
        return config;
    }

    private Map<String, String> getEditRequestConfig(Map<String, String> baseConfig, Map<String, String> newSecrets) {
        Map<String, String> editedConfig = new HashMap<>(baseConfig);
        Set<String> newSecretProperties = newSecrets.keySet();
        editedConfig.put(SecretStorageExtension.SENSITIVE_PROPERTY_LIST, String.join(",", newSecretProperties));
        editedConfig.putAll(newSecrets);
        return editedConfig;
    }

    private Map<String, String> getFilterEditRequestConfig(
            String connectorName,
            Map<String, String> preFilterEditRequestConfig,
            Set<String> propertiesToRedact,
            String newUuid) {
        Map<String, String> filteredEditedConfig = new HashMap<>(preFilterEditRequestConfig);
        filteredEditedConfig.put(SecretStorageExtension.SECRET_BUNDLE_ID, newUuid);
        propertiesToRedact.forEach(x ->
                filteredEditedConfig.computeIfPresent(x, (key, value) -> getSecretReference(connectorName, newUuid, key)));
        return filteredEditedConfig;
    }

    private Map<String, String> plainSecrets() {
        Map<String, String> secrets = new HashMap<>();
        secrets.put(SECRET_PROP_1, "secretVal1");
        secrets.put(SECRET_PROP_2, "secretVal2");
        secrets.put(SECRET_PROP_3, "secretVal3");
        return secrets;
    }


    @AfterEach
    void resetMocks() {
        reset(secretStorage, clusterState, resource);
    }

    private Response createResponse(CreateConnectorRequest createRequest) {
        String name = createRequest.name();
        Map<String, String> config = createRequest.config();
        ConnectorInfo responseEntity =
                new ConnectorInfo(name, config, null, null);
        URI createdLocation = UriBuilder.fromUri("/connectors").path(name).build();
        return Response.created(createdLocation).entity(responseEntity).build();
    }

    private Response editResponse(String connectorName, Map<String, String> config) {
        return Response.ok().entity(new ConnectorInfo(connectorName, config, null, null)).build();
    }

    @Test
    public void testFilterOnCreateConnector() throws Throwable {
        String connectorName = "testFilterOnCreateConnector";
        Map<String, String> baseConfig = getCreateRequestConfig();
        CreateConnectorRequest createRequest = new CreateConnectorRequest(connectorName, baseConfig, null);
        CreateConnectorRequest filteredCreateRequest =
                new CreateConnectorRequest(connectorName, getFilterEditRequestConfig(connectorName, baseConfig, SECRET_PROPERTIES, UUID_1), null);

        expect(secretStorage.saveSecrets(eq(connectorName), eq(plainSecrets()))).andReturn(UUID_1);
        secretStorage.markForCompletionAndDeletePrevious(eq(connectorName), eq(UUID_1));
        Response response = createResponse(filteredCreateRequest);
        expect(resource.createConnector(anyObject(), anyObject(), eq(filteredCreateRequest))).andReturn(response);
        replay(resource, secretStorage);

        HttpPost httpRequest =
                TestUtils.createHttpPostRequest(
                        restServer.serverBaseUrl(), "/connectors", OBJECT_MAPPER.writeValueAsString(createRequest));

        HttpResponse httpResponse = TestUtils.execute(httpRequest);
        verify(secretStorage, resource);

        ConnectorInfo responseEntity =
                assertResponseStatusAndGetEntity(Response.Status.CREATED.getStatusCode(), httpResponse, ConnectorInfo.class);
        assertEquals(response.getEntity(), responseEntity);
    }

    @Test
    public void testFilterCreateRequestWithNoSecrets() throws Throwable {
        String connectorName = "testFilterRequestWithNoSecrets";
        Map<String, String> mapWithNoSecrets = getCreateRequestConfig();
        SECRET_PROPERTIES.forEach(mapWithNoSecrets::remove);
        mapWithNoSecrets.remove(SecretStorageExtension.SENSITIVE_PROPERTY_LIST);

        CreateConnectorRequest createRequest = new CreateConnectorRequest(connectorName, mapWithNoSecrets, null);
        Response response = createResponse(createRequest);
        expect(resource.createConnector(anyObject(), anyObject(), eq(createRequest))).andReturn(response);
        replay(resource, secretStorage);

        HttpPost httpRequest =
                TestUtils.createHttpPostRequest(
                        restServer.serverBaseUrl(), "/connectors", OBJECT_MAPPER.writeValueAsString(createRequest));
        HttpResponse httpResponse = TestUtils.execute(httpRequest);
        verify(secretStorage, resource);

        ConnectorInfo responseEntity =
                assertResponseStatusAndGetEntity(Response.Status.CREATED.getStatusCode(), httpResponse, ConnectorInfo.class);
        assertEquals(response.getEntity(), responseEntity);
    }

    @Test
    public void testFilterOnCreateFailed() throws Throwable {
        String connectorName = "testFilterOnCreateFailed";
        CreateConnectorRequest createRequest = new CreateConnectorRequest(connectorName, getCreateRequestConfig(), null);
        CreateConnectorRequest filteredCreateRequest =
                new CreateConnectorRequest(connectorName, getFilteredCreateRequestConfig(connectorName, UUID_1), null);

        expect(secretStorage.saveSecrets(eq(connectorName), eq(plainSecrets()))).andReturn(UUID_1);
        secretStorage.deleteSecrets(eq(connectorName), eq(UUID_1));
        ErrorMessage errorMessage = new ErrorMessage(Response.Status.CONFLICT.getStatusCode(), connectorName);
        Response response = Response.status(Response.Status.CONFLICT).entity(errorMessage).build();
        expect(resource.createConnector(anyObject(), anyObject(), eq(filteredCreateRequest))).andReturn(response);
        replay(resource, secretStorage);

        HttpPost httpRequest = TestUtils.createHttpPostRequest(
                restServer.serverBaseUrl(), "/connectors", OBJECT_MAPPER.writeValueAsString(createRequest));
        HttpResponse httpResponse = TestUtils.execute(httpRequest);
        verify(resource, secretStorage);
        ErrorMessage responseEntity =
                assertResponseStatusAndGetEntity(Response.Status.CONFLICT.getStatusCode(), httpResponse, ErrorMessage.class);
        assertEquals(errorMessage, responseEntity);
    }

    @Test
    public void testFilterOnEditConnectorConfig() throws Throwable {
        String connectorName = "testFilterOnEditConnectorConfig";
        // edit configs of the "created" connector using the /connectors/{name}/config endpoint
        Map<String, String> newSecrets = plainSecrets();
        String keyToRemove = SECRET_PROP_2;
        newSecrets.put(SECRET_PROP_1, "newVal1");
        newSecrets.remove(keyToRemove);
        Set<String> newSecretProperties = new HashSet<>(SECRET_PROPERTIES);
        newSecretProperties.remove(keyToRemove);
        expect(secretStorage.getSecrets(eq(connectorName), eq(UUID_1), eq(false)))
                .andReturn(plainSecrets());
        expect(secretStorage.saveSecrets(eq(connectorName), eq(newSecrets))).andReturn(UUID_2);

        Map<String, String> baseConfig = getFilteredCreateRequestConfig(connectorName, UUID_1);
        Map<String, String> editedConfig = getEditRequestConfig(baseConfig, newSecrets);
        Map<String, String> filteredEditedConfig = getFilterEditRequestConfig(connectorName, editedConfig, newSecretProperties, UUID_2);

        Response response = editResponse(connectorName, filteredEditedConfig);
        expect(
                resource.putConnectorConfig(
                        eq(connectorName), anyObject(), anyObject(), eq(filteredEditedConfig)))
                .andReturn(response);
        secretStorage.markForCompletionAndDeletePrevious(eq(connectorName), eq(UUID_2));
        replay(secretStorage, resource);

        HttpPut httpRequest =
                TestUtils.createHttpPutRequest(
                        restServer.serverBaseUrl(),
                        "/connectors/" + connectorName + "/config",
                        OBJECT_MAPPER.writeValueAsString(editedConfig));

        HttpResponse httpResponse = TestUtils.execute(httpRequest);
        verify(secretStorage, resource);
        ConnectorInfo responseEntity =
                assertResponseStatusAndGetEntity(Response.Status.OK.getStatusCode(), httpResponse, ConnectorInfo.class);
        assertEquals(response.getEntity(), responseEntity);
    }

    @Test
    public void testFilterOnEditFailed() throws Throwable {
        String connectorName = "testFilterOnEditFailed";
        // edit configs of the "created" connector using the /connectors/{name}/config endpoint
        Map<String, String> newSecrets = plainSecrets();
        newSecrets.put(SECRET_PROP_1, "newVal1");
        newSecrets.remove(SECRET_PROP_2);
        Set<String> newSecretProperties = newSecrets.keySet();
        expect(secretStorage.getSecrets(eq(connectorName), eq(UUID_1), eq(false)))
                .andReturn(plainSecrets());
        expect(secretStorage.saveSecrets(eq(connectorName), eq(newSecrets))).andReturn(UUID_2);

        Map<String, String> baseConfig = getFilteredCreateRequestConfig(connectorName, UUID_1);
        Map<String, String> editedConfig = getEditRequestConfig(baseConfig, newSecrets);
        Map<String, String> filteredEditedConfig = getFilterEditRequestConfig(connectorName, editedConfig, newSecretProperties, UUID_2);

        ErrorMessage errorMessage = new ErrorMessage(Response.Status.CONFLICT.getStatusCode(), connectorName);
        Response response =
                Response.status(Response.Status.CONFLICT)
                        .entity(errorMessage)
                        .build();
        expect(
                resource.putConnectorConfig(
                        eq(connectorName), anyObject(), anyObject(), eq(filteredEditedConfig)))
                .andReturn(response);
        secretStorage.deleteSecrets(eq(connectorName), eq(UUID_2));
        replay(secretStorage, resource);

        HttpPut httpRequest =
                TestUtils.createHttpPutRequest(
                        restServer.serverBaseUrl(),
                        "/connectors/" + connectorName + "/config",
                        OBJECT_MAPPER.writeValueAsString(editedConfig));

        HttpResponse httpResponse = TestUtils.execute(httpRequest);
        verify(secretStorage, resource);
        ErrorMessage responseEntity =
                assertResponseStatusAndGetEntity(Response.Status.CONFLICT.getStatusCode(), httpResponse, ErrorMessage.class);
        assertEquals(errorMessage, responseEntity);
    }

    @Test
    public void testFilterOnRemoveAllSecrets() throws Throwable {
        String connectorName = "testFilterOnRemoveAllSecrets";
        // edit configs of the "created" connector using the /connectors/{name}/config endpoint
        Map<String, String> newSecrets = Collections.emptyMap();

        expect(secretStorage.getSecrets(eq(connectorName), eq(UUID_1), eq(false)))
                .andReturn(plainSecrets());
        Map<String, String> baseConfig = getFilteredCreateRequestConfig(connectorName, UUID_1);
        Map<String, String> editedConfig = getEditRequestConfig(baseConfig, newSecrets);
        Map<String, String> filteredEditedConfig = new HashMap<>(editedConfig);
        filteredEditedConfig.remove(SecretStorageExtension.SECRET_BUNDLE_ID);

        Response response =
                Response.ok()
                        .entity(new ConnectorInfo(connectorName, filteredEditedConfig, null, null))
                        .build();
        expect(
                resource.putConnectorConfig(
                        eq(connectorName), anyObject(), anyObject(), eq(filteredEditedConfig)))
                .andReturn(response);
        secretStorage.deleteSecretsAndPreviousSecrets(eq(connectorName), eq(UUID_1));
        replay(secretStorage, resource);

        HttpPut httpRequest =
                TestUtils.createHttpPutRequest(
                        restServer.serverBaseUrl(),
                        "/connectors/" + connectorName + "/config",
                        OBJECT_MAPPER.writeValueAsString(editedConfig));

        HttpResponse httpResponse = TestUtils.execute(httpRequest);
        verify(secretStorage, resource);
        ConnectorInfo responseEntity =
                assertResponseStatusAndGetEntity(Response.Status.OK.getStatusCode(), httpResponse, ConnectorInfo.class);
        assertEquals(response.getEntity(), responseEntity);
    }

    @Test
    public void testFilterOnEditWithNoSecretChanges() throws Throwable {
        String connectorName = "testFilterOnEditWithNoSecretChanges";

        // "edit" configs of the "created" connector using the /connectors/{name}/config endpoint
        expect(secretStorage.getSecrets(eq(connectorName), eq(UUID_1), eq(false)))
                .andReturn(plainSecrets());
        Map<String, String> baseConfig = getFilteredCreateRequestConfig(connectorName, UUID_1);
        Response response =
                Response.ok()
                        .entity(new ConnectorInfo(connectorName, baseConfig, null, null))
                        .build();
        expect(
                resource.putConnectorConfig(
                        eq(connectorName), anyObject(), anyObject(), eq(baseConfig)))
                .andReturn(response);
        secretStorage.deletePreviousSecrets(eq(connectorName), eq(UUID_1));
        replay(secretStorage, resource);

        HttpPut httpRequest =
                TestUtils.createHttpPutRequest(
                        restServer.serverBaseUrl(),
                        "/connectors/" + connectorName + "/config",
                        OBJECT_MAPPER.writeValueAsString(baseConfig));

        HttpResponse editResponse = TestUtils.execute(httpRequest);
        verify(secretStorage, resource);
        ConnectorInfo responseEntity =
                assertResponseStatusAndGetEntity(Response.Status.OK.getStatusCode(), editResponse, ConnectorInfo.class);
        assertEquals(response.getEntity(), responseEntity);

    }

    @Test
    public void testFilterOnDeleteConnector() throws Throwable {
        String connectorName = "testDeleteConnector";
        String baseUuid = "UUID1";
        CreateConnectorRequest filteredCreateRequest = new CreateConnectorRequest(connectorName, getFilteredCreateRequestConfig(connectorName, baseUuid), null);

        expect(clusterState.connectorConfig(eq(connectorName))).andReturn(filteredCreateRequest.config());
        secretStorage.deleteSecretsAndPreviousSecrets(eq(connectorName), eq(baseUuid));
        resource.destroyConnector(eq(connectorName), anyObject(), anyObject());
        replay(clusterState, secretStorage, resource);

        HttpDelete deleteRequest =
                TestUtils.createHttpDeleteRequest(
                        restServer.serverBaseUrl(), "/connectors/" + connectorName);
        HttpResponse deleteResponse = TestUtils.execute(deleteRequest);
        verify(clusterState, secretStorage, resource);

        assertResponseStatusAndGetEntity(Response.Status.NO_CONTENT.getStatusCode(), deleteResponse, null);
    }

    @Test
    public void testFilterOnNonFilterableRequest() throws IOException {
        HttpGet listRequest = TestUtils.createHttpGetRequest(restServer.serverBaseUrl(), "/connectors");
        Response expectedResponse = Response.notModified().build();
        expect(resource.listConnectors(anyObject(), anyObject())).andReturn(expectedResponse);
        replay(resource);
        HttpResponse response = TestUtils.execute(listRequest);
        assertEquals(expectedResponse.getStatus(), response.getStatusLine().getStatusCode());
        verify(resource);
    }

    @Test
    public void testExceptionMappingOnFilterFailure() throws Throwable {
        String connectorName = "testExceptionMappingOnFilterFailure";
        CreateConnectorRequest createRequest = new CreateConnectorRequest(connectorName, getCreateRequestConfig(), null);

        Map<String, String> baseSecrets = new HashMap<>(createRequest.config());
        baseSecrets.keySet().retainAll(SECRET_PROPERTIES);
        String concurrentModError = "Invalid SS state.";
        expect(secretStorage.saveSecrets(eq(connectorName), eq(baseSecrets))).andAnswer(() -> {
            throw new ConcurrentModificationException(concurrentModError);
        });
        replay(resource, secretStorage);

        HttpPost request =
                TestUtils.createHttpPostRequest(
                        restServer.serverBaseUrl(), "/connectors", OBJECT_MAPPER.writeValueAsString(createRequest));

        HttpResponse createResponse = TestUtils.execute(request);
        verify(secretStorage, resource);

        ErrorMessage responseError =
                assertResponseStatusAndGetEntity(Response.Status.CONFLICT.getStatusCode(), createResponse, ErrorMessage.class);
        assertNotNull(responseError);
        assertTrue(responseError.message().contains(concurrentModError));
    }

}
