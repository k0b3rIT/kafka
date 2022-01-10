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

import org.apache.kafka.connect.health.ConnectClusterState;

import com.cloudera.kafka.connect.authorization.AuthorizableAction;
import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;
import com.cloudera.kafka.connect.authorization.Operation;
import com.cloudera.kafka.connect.authorization.Resource;

import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import static com.cloudera.kafka.connect.authorization.Resource.clusterResource;
import static com.cloudera.kafka.connect.authorization.Resource.connectorResource;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class ConnectAuthorizationFilterTest {

    private static final String PRINCIPAL_NAME = "user";
    private static final String SUPER_USER_PRINCIPAL_NAME = "superUser";

    private Principal principalMock;
    private ConnectClusterState clusterStateMock;
    private ConnectAuthorizer authorizer;
    private ConnectAuthorizationFilter connectAuthorizationFilter;

    @BeforeEach
    public void setup() {
        principalMock = mock(Principal.class);
        clusterStateMock = mock(ConnectClusterState.class);
        expect(clusterStateMock.connectors()).andReturn(Collections.singletonList("connector-1")).anyTimes();
        replay(clusterStateMock);
        authorizer = mock(ConnectAuthorizer.class);
        connectAuthorizationFilter = new ConnectAuthorizationFilter(authorizer,
                clusterStateMock,
                SUPER_USER_PRINCIPAL_NAME
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.GET, HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testUnknownPathError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "unknown-path", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testGetConnectorsNoRequestAuthorizationNeeded() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/",  false, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testGetConnectorsResponseAuthorizationSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors", false, false);
        ContainerResponseContext responseContext = getMockResponse(Arrays.asList("1", "2"), Status.OK.getStatusCode(), true, "", false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, true, false),
            new AuthorizableAction(connectorResource("2"), Operation.VIEW, true, false)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext, responseContext);

        verify(authorizer, requestContext, responseContext);
    }

    @Test
    public void testGetConnectorsResponseAuthorizationSkippedForSuperUser() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors", false, false, true);
        ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
        replay(authorizer, responseContext);

        connectAuthorizationFilter.filter(requestContext, responseContext);

        verify(authorizer, requestContext, responseContext);
    }

    @Test
    public void testGetConnectorsResponseAuthorizationNoNeedSinceEmptyResult() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors", false, false);
        ContainerResponseContext responseContext = getMockResponse(Collections.emptyList(), Status.OK.getStatusCode(), true, "", false);

        connectAuthorizationFilter.filter(requestContext, responseContext);

        verify(requestContext, responseContext);
    }

    @Test
    public void testGetConnectorsExpandUnknownOk() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors?expand=unknown", false, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testGetConnectorsExpandInfoResponseAuthorizationSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors?expand=info", false, false);
        ContainerResponseContext responseContext = getMockResponse(Arrays.asList("1", "2"), Status.OK.getStatusCode(), true, "info", false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, true, false),
            new AuthorizableAction(connectorResource("2"), Operation.VIEW, true, false)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext, responseContext);

        verify(authorizer, requestContext, responseContext);
    }

    @Test
    public void testGetConnectorsExpandStatusResponseAuthorizationSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors?expand=status", false, false);
        ContainerResponseContext responseContext = getMockResponse(Arrays.asList("1", "2"), Status.OK.getStatusCode(), true, "status", false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, true, false),
            new AuthorizableAction(connectorResource("2"), Operation.VIEW, true, false)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext, responseContext);

        verify(authorizer, requestContext, responseContext);
    }

    @Test
    public void testGetConnectorsExpandStatusAndInfoResponseAuthorizationSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors?expand=status,info", false, false);
        ContainerResponseContext responseContext = getMockResponse(Arrays.asList("1", "2"), Status.OK.getStatusCode(), true, "status", false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, true, false),
            new AuthorizableAction(connectorResource("2"), Operation.VIEW, true, false)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext, responseContext);

        verify(authorizer, requestContext, responseContext);
    }

    @Test
    public void testGetConnectorsResponseAuthorizationSuccessNoResponseBecauseNoViewPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/", false, false);
        ContainerResponseContext responseContext = getMockResponse(Arrays.asList("1", "2"), Status.OK.getStatusCode(), false, "", false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, true, false),
            new AuthorizableAction(connectorResource("2"), Operation.VIEW, true, false)));
        setupComplexMockAuthorizer(actions, Collections.emptySet());

        connectAuthorizationFilter.filter(requestContext, responseContext);

        verify(authorizer, requestContext, responseContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testGetConnectorsResponseAuthorizationWrongHttpMethodNoAuth(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/", false, false);
        ContainerResponseContext responseContext = getMockResponse(Arrays.asList("1", "2"), Status.OK.getStatusCode(), false, "", true);

        connectAuthorizationFilter.filter(requestContext, responseContext);

        verify(requestContext, responseContext);
    }

    @Test
    public void testGetConnectorsResponseAuthorizationWrongHttpStatusNoAuth() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/", false, false);
        ContainerResponseContext responseContext = getMockResponse(Arrays.asList("1", "2"), Status.BAD_REQUEST.getStatusCode(), false, "", true);

        connectAuthorizationFilter.filter(requestContext, responseContext);

        verify(requestContext, responseContext);
    }

    @Test
    public void testPostConnectorTaskIsForbiddenForNonSuperUserPrincipal() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.POST, "connectors/c1/tasks/", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testPostConnectorTaskIsAllowedForSuperUserPrincipal() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.POST, "connectors/c1/tasks/", false, false, true);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testRootSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "", false, false);
        setupSimpleMockAuthorizer(clusterResource(), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testRootSuccessWithAnonymousUser() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "", false, false, ConnectAuthorizer.ANONYMOUS_PRINCIPAL_NAME);
        setupSimpleMockAuthorizer(clusterResource(), Operation.VIEW, true, ConnectAuthorizer.ANONYMOUS_PRINCIPAL_NAME);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testRootNoViewPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "", true, false);
        setupSimpleMockAuthorizer(clusterResource(), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testRootWrongWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testConnectorPluginSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connector-plugins/", false, false);
        setupSimpleMockAuthorizer(clusterResource(), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testConnectorPluginNoViewPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connector-plugins/", true, false);
        setupSimpleMockAuthorizer(clusterResource(), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testConnectorPluginWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connector-plugins/", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testConnectorPluginConfigSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connector-plugins/cp/config", false, false);
        setupSimpleMockAuthorizer(clusterResource(), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testConnectorPluginConfigNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connector-plugins/cp/config", true, false);
        setupSimpleMockAuthorizer(clusterResource(), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testConnectorPluginConfigWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connector-plugins/cp/config", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testConnectorPluginValidateSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connector-plugins/cp/config/validate", false, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(clusterResource(), Operation.VIEW, false, false),
            new AuthorizableAction(clusterResource(), Operation.VALIDATE)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testConnectorPluginValidateNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connector-plugins/cp/config/validate", true, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(clusterResource(), Operation.VIEW, false, false),
            new AuthorizableAction(clusterResource(), Operation.VALIDATE)));
        setupComplexMockAuthorizer(actions, Collections.emptySet());

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testConnectorPluginValidateWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connector-plugins/cp/config/validate", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testGetLoggerSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "admin/loggers", false, false);
        setupSimpleMockAuthorizer(clusterResource(), Operation.MANAGE, true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testGetLoggerNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "admin/loggers", true, false);
        setupSimpleMockAuthorizer(clusterResource(), Operation.MANAGE, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testPutLoggerSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "admin/loggers/logger/", false, false);
        setupSimpleMockAuthorizer(clusterResource(), Operation.MANAGE, true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testPutLoggerNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "admin/loggers/logger/", true, false);
        setupSimpleMockAuthorizer(clusterResource(), Operation.MANAGE, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testLoggerWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "admin/loggers", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testGetConnectorSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1", false, false);
        setupSimpleMockAuthorizer(connectorResource("1"), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testGetConnectorNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1", true, false);
        setupSimpleMockAuthorizer(connectorResource("1"), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testDeleteConnectorSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.DELETE, "connectors/1", false, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.DELETE)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testDeleteConnectorNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.DELETE, "connectors/1", true, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.DELETE)));
        setupComplexMockAuthorizer(actions, Collections.emptySet());

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.POST, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testSimpleConnectorWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testGetConnectorConfigSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/config", false, false);
        setupSimpleMockAuthorizer(connectorResource("1"), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testGetConnectorConfigNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/config", true, false);
        setupSimpleMockAuthorizer(connectorResource("1"), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testEditConnectorConfigSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connectors/connector-1/config", false, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("connector-1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("connector-1"), Operation.EDIT)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testEditConnectorConfigCreateSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connectors/1/config", false, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.CREATE)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testEditConnectorConfigCreateSuccessWithSuperUser() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connectors/1/config", false, false, true);
        replay(authorizer);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testEditConnectorConfigNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connectors/connector-1/config", true, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("connector-1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("connector-1"), Operation.EDIT)));
        setupComplexMockAuthorizer(actions, Collections.emptySet());

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testConnectorConfigWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/config", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testTaskConfigSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/tasks-config", false, false);
        setupSimpleMockAuthorizer(connectorResource("1"), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testTaskConfigNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/tasks-config", true, false);
        setupSimpleMockAuthorizer(connectorResource("1"), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testTaskrConfigWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/tasks-config", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testConnectorStatusSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/status", false, false);
        setupSimpleMockAuthorizer(connectorResource("1"), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testConnectorStatusNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/status", true, false);
        setupSimpleMockAuthorizer(connectorResource("1"), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testConnectorStatusWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/status", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testTopicsSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/topics", false, false);
        setupSimpleMockAuthorizer(connectorResource("1"), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testTopicsNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/topics", true, false);
        setupSimpleMockAuthorizer(connectorResource("1"), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testTopicsWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/topics", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testTopicResetSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connectors/1/topics/reset", false, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.MANAGE)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testTopicResetNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connectors/1/topics/reset", true, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.MANAGE)));
        setupComplexMockAuthorizer(actions, Collections.emptySet());

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testTopicResetWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/topics/reset", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {"pause", "resume"})
    public void testConnectorManageSuccess(String pathEnd) {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connectors/1/" + pathEnd,
            false, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.MANAGE),
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {"pause", "resume"})
    public void testConnectorManageNoPermission(String pathEnd) {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.PUT, "connectors/1/" + pathEnd,
            true, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.MANAGE)));
        setupComplexMockAuthorizer(actions, Collections.emptySet());

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testConnectorManageWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/pause", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testConnectorRestartSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.POST, "connectors/1/restart", false, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.MANAGE)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testConnectorRestartNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.POST, "connectors/1/restart", true, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.MANAGE)));
        setupComplexMockAuthorizer(actions, Collections.emptySet());

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testConnectorRestartWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/restart", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testTasksSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/tasks", false, false);
        setupSimpleMockAuthorizer(connectorResource("1"), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testTasksNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/tasks", true, false);
        setupSimpleMockAuthorizer(connectorResource("1"), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testTasksWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/tasks", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testTasksStatusSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/tasks/1/status", false, false);
        setupSimpleMockAuthorizer(connectorResource("1"), true);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testTasksStatusNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.GET, "connectors/1/tasks/1/status", true, false);
        setupSimpleMockAuthorizer(connectorResource("1"), false);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testTasksStatusWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/tasks/1/status", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testTasksRestartSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.POST, "connectors/1/tasks/1/restart", false, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.MANAGE)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testTasksRestartNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.POST, "connectors/1/tasks/1/restart", true, false);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("1"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("1"), Operation.MANAGE)));
        setupComplexMockAuthorizer(actions, Collections.emptySet());

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testTasksRestartWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/tasks/1/restart", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testCreateConnectorSuccess() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.POST, "connectors/", false, true);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("new-connector"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("new-connector"), Operation.CREATE)));
        setupComplexMockAuthorizer(actions, actions);

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @Test
    public void testCreateConnectorNoNameError() {
        ContainerRequestContext requestContext = getMockNotValidCreateRequest();

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @Test
    public void testCreateConnectorNoPermission() {
        ContainerRequestContext requestContext = getMockRequest(HttpMethod.POST, "connectors/", true, true);
        Set<AuthorizableAction> actions = new HashSet<>(Arrays.asList(
            new AuthorizableAction(connectorResource("new-connector"), Operation.VIEW, false, false),
            new AuthorizableAction(connectorResource("new-connector"), Operation.CREATE)));
        setupComplexMockAuthorizer(actions, Collections.emptySet());

        connectAuthorizationFilter.filter(requestContext);

        verify(authorizer, requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testCreateConnectorWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.PUT, HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.PATCH, HttpMethod.HEAD})
    public void testConnectorFenceWrongHttpMethodError(String method) {
        ContainerRequestContext requestContext = getMockRequest(method, "connectors/1/fence", true, false);

        connectAuthorizationFilter.filter(requestContext);

        verify(requestContext);
    }

    private void setupSimpleMockAuthorizer(Resource resource, boolean isAuthorized) {
        setupSimpleMockAuthorizer(resource, Operation.VIEW, isAuthorized);
    }

    private void setupSimpleMockAuthorizer(Resource resource, Operation operation, boolean isAuthorized) {
        expect(authorizer.isAuthorized(principalMock, new AuthorizableAction(resource, operation))).andReturn(isAuthorized);
        replay(authorizer);
    }

    private void setupSimpleMockAuthorizer(Resource resource, Operation operation, boolean isAuthorized, String principalName) {
        expect(authorizer.isAuthorized(principalNameMatcher(principalName), eq(new AuthorizableAction(resource, operation)))).andReturn(isAuthorized);
        replay(authorizer);
    }

    private Principal principalNameMatcher(String principalName) {
        EasyMock.reportMatcher(
                new IArgumentMatcher() {
                    @Override
                    public boolean matches(Object argument) {
                        return argument instanceof Principal && Objects.equals(principalName, ((Principal) argument).getName());
                    }

                    @Override
                    public void appendTo(StringBuffer buffer) {
                        buffer.append("Principal(name=").append(principalName).append(")");
                    }
                }
        );
        return null;
    }

    private void setupComplexMockAuthorizer(Set<AuthorizableAction> actions, Set<AuthorizableAction> authorizedActions) {
        expect(authorizer.filterAuthorized(principalMock, actions)).andReturn(authorizedActions);
        replay(authorizer);
    }

    private ContainerRequestContext getMockRequest(String httpMethod, String path, boolean isExceptionCase, boolean isRequestBodyNeeded) {
        return getMockRequest(httpMethod, path, isExceptionCase, isRequestBodyNeeded, false);
    }

    private ContainerRequestContext getMockRequest(String httpMethod,
                                                   String path,
                                                   boolean isExceptionCase,
                                                   boolean isRequestBodyNeeded,
                                                   boolean superUser) {
        return getMockRequest(httpMethod, path, isExceptionCase, isRequestBodyNeeded,
                superUser ? SUPER_USER_PRINCIPAL_NAME : PRINCIPAL_NAME);
    }

    private ContainerRequestContext getMockRequest(String httpMethod,
                                                   String path,
                                                   boolean isExceptionCase,
                                                   boolean isRequestBodyNeeded,
                                                   String username) {
        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        UriInfo uriInfo = mock(UriInfo.class);
        SecurityContext securityContext = mock(SecurityContext.class);
        String[] pathQueryArray = path.split("\\?");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        if (pathQueryArray.length == 2) {
            path = pathQueryArray[0];
            String[] keyValueArray = pathQueryArray[1].split("=");
            String[] valueArray = keyValueArray[1].split(",");
            for (String value : valueArray) {
                queryParams.add(keyValueArray[0], value);
            }
        }

        expect(requestContext.getMethod()).andReturn(httpMethod).anyTimes();
        expect(uriInfo.getPath()).andReturn(path).anyTimes();
        expect(uriInfo.getQueryParameters()).andReturn(queryParams).anyTimes();
        expect(requestContext.getUriInfo()).andReturn(uriInfo).anyTimes();
        expect(securityContext.getUserPrincipal()).andReturn(principalMock).anyTimes();
        expect(requestContext.getSecurityContext()).andReturn(securityContext).anyTimes();
        expect(principalMock.getName()).andReturn(username).anyTimes();

        if (isExceptionCase) {
            requestContext.abortWith(anyObject(Response.class));
            EasyMock.expectLastCall();
        }

        if (isRequestBodyNeeded) {
            String json = "{\"name\":\"new-connector\", \"config\":null}";
            expect(requestContext.getEntityStream()).andReturn(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
            requestContext.setEntityStream(anyObject());
        }

        replay(uriInfo, principalMock, securityContext, requestContext);
        return requestContext;
    }

    private ContainerRequestContext getMockNotValidCreateRequest() {
        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        UriInfo uriInfo = mock(UriInfo.class);
        SecurityContext securityContext = mock(SecurityContext.class);

        expect(requestContext.getMethod()).andReturn(HttpMethod.POST).anyTimes();
        expect(uriInfo.getPath()).andReturn("connectors/").anyTimes();
        expect(uriInfo.getQueryParameters()).andReturn(new MultivaluedHashMap<>()).anyTimes();
        expect(requestContext.getUriInfo()).andReturn(uriInfo).anyTimes();
        expect(securityContext.getUserPrincipal()).andReturn(principalMock).anyTimes();
        expect(requestContext.getSecurityContext()).andReturn(securityContext).anyTimes();
        expect(principalMock.getName()).andReturn(PRINCIPAL_NAME).anyTimes();

        requestContext.abortWith(anyObject(Response.class));
        EasyMock.expectLastCall();

        String json = "invalid-json";
        expect(requestContext.getEntityStream()).andReturn(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        requestContext.setEntityStream(anyObject());

        replay(uriInfo, principalMock, securityContext, requestContext);
        return requestContext;
    }

    private ContainerResponseContext getMockResponse(List<String> connectorNames, int httpStatus, boolean isAllActionAuthorizable, String expand, boolean isExceptionCase) {
        ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
        Map<String, Map<String, Object>> output = connectorNames.stream().collect(Collectors.toMap(name -> name, name -> Collections.emptyMap()));

        expect(responseContext.getStatus()).andReturn(httpStatus).anyTimes();

        if (!isExceptionCase) {
            switch (expand) {
                case "info":
                case "status":
                    expect(responseContext.getEntity()).andReturn(output);
                    break;
                default:
                    expect(responseContext.getEntity()).andReturn(connectorNames);
                    break;
            }

            if (!connectorNames.isEmpty()) {
                switch (expand) {
                    case "info":
                    case "status":
                        if (isAllActionAuthorizable) {
                            responseContext.setEntity(output);
                        } else {
                            responseContext.setEntity(Collections.emptyMap());
                        }
                        break;
                    default:
                        if (isAllActionAuthorizable) {
                            responseContext.setEntity(anyObject(Set.class));
                        } else {
                            responseContext.setEntity(Collections.emptySet());
                        }
                        break;
                }

                EasyMock.expectLastCall();
            }
        }

        replay(responseContext);
        return responseContext;
    }

}
