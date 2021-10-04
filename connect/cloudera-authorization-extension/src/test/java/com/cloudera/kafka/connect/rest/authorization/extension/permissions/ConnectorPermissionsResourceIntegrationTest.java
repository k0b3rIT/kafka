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
package com.cloudera.kafka.connect.rest.authorization.extension.permissions;

import com.cloudera.kafka.connect.authorization.AuthorizableAction;
import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;
import com.cloudera.kafka.connect.authorization.Operation;
import com.cloudera.kafka.connect.authorization.Resource;
import com.cloudera.kafka.connect.rest.authorization.extension.TestRestServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.runtime.health.ConnectClusterStateImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.getCurrentArgument;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorPermissionsResourceIntegrationTest {

    public static final String TEST_CONNECTOR_NAME = "test_connector_name";
    private static final Set<AuthorizableAction> CONNECTOR_ACTIONS = new HashSet<>(Arrays.asList(
        new AuthorizableAction(Resource.connectorResource(TEST_CONNECTOR_NAME), Operation.VIEW, false, false),
        new AuthorizableAction(Resource.connectorResource(TEST_CONNECTOR_NAME), Operation.MANAGE, false, false),
        new AuthorizableAction(Resource.connectorResource(TEST_CONNECTOR_NAME), Operation.EDIT, false, false),
        new AuthorizableAction(Resource.connectorResource(TEST_CONNECTOR_NAME), Operation.DELETE, false, false),
        new AuthorizableAction(Resource.clusterResource(), Operation.VALIDATE, false, false)
    ));

    private ConnectClusterStateImpl clusterState;
    private TestRestServer restServer;
    private ConnectAuthorizer authorizer;

    @BeforeEach
    public void setup() {
        authorizer = mock(ConnectAuthorizer.class);
        clusterState = mock(ConnectClusterStateImpl.class);
        ConnectorPermissionsService service = new ConnectorPermissionsService(authorizer);
        ConnectorPermissionsResource resource = new ConnectorPermissionsResource(clusterState, service);
        restServer = new TestRestServer();
        restServer.initializeResources(Collections.singletonList(resource));
    }

    @AfterEach
    public void tearDown() {
        if (restServer != null) {
            restServer.stop();
        }
    }

    @Test
    void getConnectorPermissions_SingleConnector_AllPermission() throws IOException {
        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andAnswer(() -> getCurrentArgument(1));
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_CONNECTOR_NAME));
        replay(authorizer, clusterState);

        HttpResponse response = executeGetConnectorPermissions();

        // verify the authorizer now as if verification fails response is Http Error 500
        verify(authorizer);

        ConnectorPermissions responseEntity = unwrapEntity(response);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(getAllPermissions(), responseEntity.getPermissions());
        assertTrue(responseEntity.isCreateAllowed());
    }

    @Test
    void getConnectorPermissions_SingleConnector_NoPermission() throws IOException {
        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andReturn(Collections.emptySet());
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_CONNECTOR_NAME));
        replay(authorizer, clusterState);

        HttpResponse response = executeGetConnectorPermissions();

        // verify the authorizer now as if verification fails response is Http Error 500
        verify(authorizer);

        ConnectorPermissions responseEntity = unwrapEntity(response);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(new HashMap<String, Permissions>(), responseEntity.getPermissions());
        assertFalse(responseEntity.isCreateAllowed());
    }

    private HashMap<String, Permissions> getAllPermissions() {
        final HashMap<String, Permissions> permissions = new HashMap<>();
        permissions.put(TEST_CONNECTOR_NAME, new Permissions(true, true, true));
        return permissions;
    }

    private static ConnectorPermissions unwrapEntity(HttpResponse response) throws IOException {
        return new ObjectMapper().readValue(
                EntityUtils.toString(response.getEntity()),
                ConnectorPermissions.class
        );
    }

    private HttpResponse executeGetConnectorPermissions() throws IOException {
        return execute(new HttpGet(restServer.serverBaseUrl() + "/connector-permissions"));
    }

    private HttpResponse execute(HttpRequestBase request) throws IOException {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            return client.execute(request);
        }
    }
}
