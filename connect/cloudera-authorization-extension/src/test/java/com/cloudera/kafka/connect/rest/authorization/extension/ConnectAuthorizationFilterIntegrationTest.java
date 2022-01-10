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

import org.apache.kafka.connect.runtime.health.ConnectClusterStateImpl;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.entities.ServerInfo;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.runtime.rest.resources.LoggingResource;
import org.apache.kafka.connect.runtime.rest.resources.RootResource;

import com.cloudera.kafka.connect.authorization.AuthorizableAction;
import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;
import com.cloudera.kafka.connect.authorization.Operation;
import com.cloudera.kafka.connect.common.TestConnectRestServer;
import com.cloudera.kafka.connect.common.TestUtils;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;

import static com.cloudera.kafka.connect.authorization.Resource.clusterResource;
import static com.cloudera.kafka.connect.authorization.Resource.connectorResource;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectAuthorizationFilterIntegrationTest {
    private static final String NORMAL_USER = "user";
    private static final String SUPER_USER_PRINCIPAL_NAME = "superUser";

    private ConnectClusterStateImpl clusterState;
    private TestConnectRestServer restServer;
    private ConnectAuthorizer authorizer;
    private AuthenticationFilter authenticator;
    private ConnectAuthorizationFilter filter;
    private Principal principal;

    @BeforeEach
    public void setup() {
        principal = mock(Principal.class);
        clusterState = mock(ConnectClusterStateImpl.class);
        authorizer = mock(ConnectAuthorizer.class);
        authenticator = new AuthenticationFilter(principal);
        restServer = new TestConnectRestServer();
        restServer.start();
        filter = new ConnectAuthorizationFilter(authorizer, clusterState, SUPER_USER_PRINCIPAL_NAME);
    }

    @AfterEach
    public void tearDown() {
        if (restServer != null) {
            restServer.stop();
        }
    }

    @Test
    public void testUnknownPath() throws IOException {
        restServer.initializeResources(Arrays.asList(authenticator, filter));
        expect(principal.getName()).andReturn(NORMAL_USER).anyTimes();
        replay(principal);
        HttpGet request = TestUtils.createHttpGetRequest(restServer.serverBaseUrl(), "/unknown-path");

        HttpResponse response = TestUtils.execute(request);

        assertEquals(404, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testRootResource() throws IOException {
        RootResource resource = mock(RootResource.class);
        ServerInfo expectedResult = new ServerInfo("");
        expect(principal.getName()).andReturn(NORMAL_USER).anyTimes();
        expect(resource.serverInfo()).andReturn(expectedResult);
        expect(authorizer.isAuthorized(principal, new AuthorizableAction(clusterResource(), Operation.VIEW))).andReturn(true);
        restServer.initializeResources(Arrays.asList(resource, authenticator, filter));
        HttpGet request = TestUtils.createHttpGetRequest(restServer.serverBaseUrl(), "/");
        replay(resource, authorizer, principal);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertTrue(EntityUtils.toString(response.getEntity()).contains("\"kafka_cluster_id\":\"\""));
    }

    @Test
    public void testListConnectorsResponseIsEmptyList() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        expect(resource.listConnectors(anyObject(), anyObject())).andReturn(Response.ok(Collections.emptyList()).build());
        HttpGet request = createConnectorsGetRequest(resource, null, false, "/connectors/");

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("[]", EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void testListConnectorsSuccess() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        expect(resource.listConnectors(anyObject(), anyObject())).andReturn(Response.ok(Collections.singletonList("1")).build());
        HttpGet request = createConnectorsGetRequest(resource,
            Collections.singleton(new AuthorizableAction(connectorResource("1"), Operation.VIEW)),
            false, "/connectors/");

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("[\"1\"]", EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void testListConnectorsNoAuthorizationSinceUserIsSuperUser() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        expect(resource.listConnectors(anyObject(), anyObject())).andReturn(Response.ok(Collections.emptyList()).build());
        HttpGet request = createConnectorsGetRequest(resource, null, false, "/connectors/");

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("[]", EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void testListConnectorsResponseIsEmptyMap() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        expect(resource.listConnectors(anyObject(), anyObject())).andReturn(Response.ok(Collections.emptyMap()).build());
        HttpGet request = createConnectorsGetRequest(resource, null, false, "/connectors?expand=info");

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{}", EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void testListConnectorsExpandInfo() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW),
            new AuthorizableAction(connectorResource("2"), Operation.VIEW)));
        HttpGet request = createConnectorsGetRequest(resource, actions, true, "/connectors?expand=info");

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{\"1\":null,\"2\":null}", EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void testListConnectorsExpandStatus() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW),
            new AuthorizableAction(connectorResource("2"), Operation.VIEW)));
        HttpGet request = createConnectorsGetRequest(resource, actions, true, "/connectors?expand=status");

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{\"1\":null,\"2\":null}", EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void testListConnectorsExpandStatusInfo() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW),
            new AuthorizableAction(connectorResource("2"), Operation.VIEW)));
        HttpGet request = createConnectorsGetRequest(resource, actions, true, "/connectors?expand=status&expand=info");

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{\"1\":null,\"2\":null}", EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void testListConnectorsExpandInfoOnlyOneValid() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        Set<AuthorizableAction> actions = new HashSet<>(Collections.singletonList(new AuthorizableAction(connectorResource("1"), Operation.VIEW)));
        HttpGet request = createConnectorsGetRequest(resource, actions, true, "/connectors?expand=info");

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{\"1\":null}", EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void testListConnectorsExpandStatusOnlyOneValid() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        Set<AuthorizableAction> actions = new HashSet<>(Collections.singletonList(new AuthorizableAction(connectorResource("1"), Operation.VIEW)));
        HttpGet request = createConnectorsGetRequest(resource, actions, true, "/connectors?expand=status");

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{\"1\":null}", EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void testListConnectorsExpandUnknown() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        Set<AuthorizableAction> actions = new HashSet<>(Collections.singletonList(new AuthorizableAction(connectorResource("1"), Operation.VIEW)));
        HttpGet request = createConnectorsGetRequest(resource, actions, true, "/connectors?expand=unknown-expand");

        HttpResponse response = TestUtils.execute(request);

        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{\"1\":null}", EntityUtils.toString(response.getEntity()));
        verify(resource, authorizer, principal);
    }

    @Test
    public void testListConnectorsExpandUnknownAndKnown() throws IOException {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        Set<AuthorizableAction> actions = new HashSet<>(Collections.singletonList(new AuthorizableAction(connectorResource("1"), Operation.VIEW)));
        HttpGet request = createConnectorsGetRequest(resource, actions, true, "/connectors?expand=status,unknown-expand");

        HttpResponse response = TestUtils.execute(request);

        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{\"1\":null}", EntityUtils.toString(response.getEntity()));
        verify(resource, authorizer, principal);
    }

    @Test
    public void testPostConnectorSuccess() throws Throwable {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        HttpPost request = createConnectorPostRequest(resource, true);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testPostConnectorSuccessWithSuperUser() throws Throwable {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        HttpPost request = createConnectorPostRequest(resource, true, true);

        HttpResponse response = TestUtils.execute(request);

        assertEquals(200, response.getStatusLine().getStatusCode());
        verify(resource, authorizer, principal);
    }

    @Test
    public void testPostConnectorBadRequest() throws Throwable {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        HttpPost request = createConnectorPostRequest(resource, false);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(400, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testPutConnectorConfigExists() throws Throwable {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        HttpPut request = createPutConnectorConfigRequest(resource, Operation.EDIT);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal, clusterState);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testPutConnectorConfigCreated() throws Throwable {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        HttpPut request = createPutConnectorConfigRequest(resource, Operation.CREATE);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal, clusterState);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testListLoggersSuccess() throws IOException {
        LoggingResource resource = mock(LoggingResource.class);
        HttpGet request = createListLoggersRequest(resource, true);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testListLoggersNotFound() throws IOException {
        LoggingResource resource = mock(LoggingResource.class);
        HttpGet request = createListLoggersRequest(resource, false);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(404, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testValidateConnectorPluginsResourceRequestSuccess() throws Throwable {
        ConnectorPluginsResource resource = mock(ConnectorPluginsResource.class);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(clusterResource(), Operation.VALIDATE),
            new AuthorizableAction(clusterResource(), Operation.VIEW, false, false)));
        HttpPut request = createValidateConfigRequest(resource, actions, true);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testValidateConnectorPluginsResourceRequestForbidden() throws Throwable {
        ConnectorPluginsResource resource = mock(ConnectorPluginsResource.class);
        HttpPut request = createValidateConfigRequest(resource,
            Collections.singleton(new AuthorizableAction(clusterResource(), Operation.VIEW, false, false)),
            false);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(403, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testValidateConnectorPluginsResourceRequestNotFound() throws Throwable {
        ConnectorPluginsResource resource = mock(ConnectorPluginsResource.class);
        HttpPut request = createValidateConfigRequest(resource, Collections.emptySet(), false);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(404, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testFenceConnectorResourceRequestForbidden() throws Throwable {
        ConnectorsResource resource = mock(ConnectorsResource.class);
        restServer.initializeResources(Arrays.asList(resource, authenticator, filter));
        expect(principal.getName()).andReturn(NORMAL_USER).anyTimes();
        replay(resource, authorizer, principal);

        HttpResponse response = TestUtils.execute(TestUtils.createHttpPutRequest(restServer.serverBaseUrl(), "/connectors/1/fence"));

        verify(authorizer, resource, principal);
        assertEquals(403, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testGetConnectorPluginsConfigResourceRequestSuccess() throws Throwable {
        ConnectorPluginsResource resource = mock(ConnectorPluginsResource.class);
        HttpGet request = createGetPluginConfigRequest(resource, true);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testGetConnectorPluginsConfigResourceRequestNotFound() throws Throwable {
        ConnectorPluginsResource resource = mock(ConnectorPluginsResource.class);
        HttpGet request = createGetPluginConfigRequest(resource, false);

        HttpResponse response = TestUtils.execute(request);

        verify(authorizer, resource, principal);
        assertEquals(404, response.getStatusLine().getStatusCode());
    }

    private HttpGet createConnectorsGetRequest(ConnectorsResource resource, Set<AuthorizableAction> actions, boolean isResultRequired, String path) throws MalformedURLException {
        if (null != actions) {
            expect(authorizer.filterAuthorized(anyObject(), anyObject())).andReturn(actions);
        }
        if (isResultRequired) {
            Map<String, Object> expectedResult = new HashMap<>();
            expectedResult.put("1", null);
            expectedResult.put("2", null);
            expect(resource.listConnectors(anyObject(), anyObject())).andReturn(Response.ok(expectedResult).build());
        }
        expect(principal.getName()).andReturn(NORMAL_USER).anyTimes();

        replay(resource, authorizer, principal);
        restServer.initializeResources(Arrays.asList(resource, authenticator, filter));
        return TestUtils.createHttpGetRequest(restServer.serverBaseUrl(), path);
    }

    private HttpPost createConnectorPostRequest(ConnectorsResource resource, boolean isSuccessful) throws Throwable {
        return createConnectorPostRequest(resource, isSuccessful, false);
    }

    private HttpPost createConnectorPostRequest(ConnectorsResource resource,
                                                boolean isSuccessful,
                                                boolean superUser) throws Throwable {
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("new-connector"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("new-connector"), Operation.CREATE)));
        String json;

        expect(principal.getName()).andReturn(superUser ? SUPER_USER_PRINCIPAL_NAME : NORMAL_USER).anyTimes();
        if (isSuccessful) {
            json = "{\"name\":\"new-connector\", \"config\":{\"topic\":\"test\"}}";
            Map<String, String> expectedConfig = new HashMap<>();
            expectedConfig.put("topic", "test");
            CreateConnectorRequest expectedRequest = new CreateConnectorRequest(
                    "new-connector",
                    expectedConfig,
                    null
            );
            expect(resource.createConnector(anyObject(), anyObject(), eq(expectedRequest))).andReturn(Response.ok().build());
            if (!superUser) {
                expect(authorizer.filterAuthorized(principal, actions)).andReturn(actions);
            }
        } else {
            json = "{\"name\":\"\", \"config\":null}";
        }

        restServer.initializeResources(Arrays.asList(resource, authenticator, filter));
        replay(resource, authorizer, principal);
        return TestUtils.createHttpPostRequest(restServer.serverBaseUrl(), "/connectors", json);
    }

    private HttpPut createPutConnectorConfigRequest(ConnectorsResource resource, Operation operation) throws Throwable {
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("name"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("name"), operation)));
        expect(principal.getName()).andReturn(NORMAL_USER).anyTimes();
        expect(resource.putConnectorConfig(anyObject(), anyObject(), anyObject(), anyObject())).andReturn(Response.ok().build());
        expect(authorizer.filterAuthorized(principal, actions)).andReturn(actions);

        if (Operation.CREATE.equals(operation)) {
            expect(clusterState.connectors()).andReturn(Collections.emptyList());
        } else if (Operation.EDIT.equals(operation)) {
            expect(clusterState.connectors()).andReturn(Collections.singletonList("name"));
        }

        restServer.initializeResources(Arrays.asList(resource, authenticator, filter));
        replay(resource, authorizer, principal, clusterState);
        return TestUtils.createHttpPutRequest(restServer.serverBaseUrl(), "/connectors/name/config");
    }

    private HttpGet createListLoggersRequest(LoggingResource resource, boolean isAuthorized) throws MalformedURLException {
        AuthorizableAction action = new AuthorizableAction(clusterResource(), Operation.MANAGE);
        expect(principal.getName()).andReturn(NORMAL_USER).anyTimes();
        expect(authorizer.isAuthorized(principal, action)).andReturn(isAuthorized);

        if (isAuthorized) {
            expect(resource.listLoggers()).andReturn(Response.ok().build());
        }

        restServer.initializeResources(Arrays.asList(resource, authenticator, filter));
        replay(resource, authorizer, principal);
        return TestUtils.createHttpGetRequest(restServer.serverBaseUrl(), "/admin/loggers/");
    }

    private HttpPut createValidateConfigRequest(ConnectorPluginsResource resource, Set<AuthorizableAction> authorizedActions, boolean isSuccessful) throws Throwable {
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(clusterResource(), Operation.VALIDATE),
            new AuthorizableAction(clusterResource(), Operation.VIEW, false, false)));

        expect(principal.getName()).andReturn(NORMAL_USER).anyTimes();
        expect(authorizer.filterAuthorized(principal, actions)).andReturn(authorizedActions);
        if (isSuccessful) {
            expect(resource.validateConfigs(anyObject(), anyObject()))
                .andReturn(new ConfigInfos("name", 0, Collections.emptyList(), Collections.emptyList()));
        }

        restServer.initializeResources(Arrays.asList(resource, authenticator, filter));
        replay(resource, authorizer, principal);
        return TestUtils.createHttpPutRequest(restServer.serverBaseUrl(), "/connector-plugins/connector-type/config/validate");
    }

    private HttpGet createGetPluginConfigRequest(ConnectorPluginsResource resource, boolean isAuthorized) throws Throwable {
        AuthorizableAction action = new AuthorizableAction(clusterResource(), Operation.VIEW, true, true);

        expect(principal.getName()).andReturn(NORMAL_USER).anyTimes();
        expect(authorizer.isAuthorized(principal, action)).andReturn(isAuthorized);
        if (isAuthorized) {
            expect(resource.getConnectorConfigDef(anyString()))
                .andReturn(Collections.singletonList(new ConfigKeyInfo("cp", "", false, "", "", "", "", 0, "", "", Collections.emptyList())));
        }

        restServer.initializeResources(Arrays.asList(resource, authenticator, filter));
        replay(resource, authorizer, principal);
        return TestUtils.createHttpGetRequest(restServer.serverBaseUrl(), "/connector-plugins/cp/config");
    }

    @Provider
    @PreMatching
    @Priority(Priorities.AUTHENTICATION)
    public static class AuthenticationFilter implements ContainerRequestFilter {

        private final Principal principal;

        private AuthenticationFilter(Principal principal) {
            this.principal = principal;
        }

        @Override
        public void filter(ContainerRequestContext requestContext) {
            requestContext.setSecurityContext(new SecurityContext() {
                @Override
                public Principal getUserPrincipal() {
                    return principal;
                }

                @Override
                public boolean isUserInRole(String role) {
                    return "privileged".equals(role);
                }

                @Override
                public boolean isSecure() {
                    return true;
                }

                @Override
                public String getAuthenticationScheme() {
                    return "BASIC";
                }
            });
        }
    }

}
