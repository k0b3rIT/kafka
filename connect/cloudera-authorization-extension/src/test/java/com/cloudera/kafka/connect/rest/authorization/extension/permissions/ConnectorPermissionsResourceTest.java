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

package com.cloudera.kafka.connect.rest.authorization.extension.permissions;

import com.cloudera.kafka.connect.authorization.AuthorizableAction;
import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;
import com.cloudera.kafka.connect.authorization.Operation;
import com.cloudera.kafka.connect.authorization.Resource;
import org.apache.kafka.connect.runtime.health.ConnectClusterStateImpl;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorPermissionsResourceTest {
    public static final String TEST_RESOURCE_NAME = "TEST_RESOURCE_NAME";
    public static final String USER = "user";

    private static final AuthorizableAction CLUSTER_VALIDATE_ACTION = new AuthorizableAction(Resource.clusterResource(), Operation.VALIDATE, false, false);
    private static final Set<AuthorizableAction> CONNECTOR_ACTIONS = new HashSet<>(Arrays.asList(
        new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.VIEW, false, false),
        new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.MANAGE, false, false),
        new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.EDIT, false, false),
        new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.DELETE, false, false),
        CLUSTER_VALIDATE_ACTION
    ));

    private ConnectorPermissionsResource resource;
    private ConnectClusterStateImpl clusterState;
    private ConnectAuthorizer authorizer;

    private static Stream<Arguments> listConnectorPermissions_success() {
        return Stream.of(
                Arguments.of(false, new Permissions(false, true, false), viewableAction(new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.DELETE))),
                Arguments.of(false, new Permissions(true, false, false), viewableAction(new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.EDIT))),
                Arguments.of(false, new Permissions(false, false, true), viewableAction(new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.MANAGE)))
        );
    }

    private static ConnectorPermissions unwrapEntity(Response response) {
        return (ConnectorPermissions) response.getEntity();
    }

    private static Set<AuthorizableAction> viewableAction(AuthorizableAction action) {
        return new HashSet<>(Arrays.asList(
                action,
                new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.VIEW))
        );
    }

    private static SecurityContext mockSecurityContext(String principalName) {
        SecurityContext ctx = EasyMock.createMock(SecurityContext.class);
        Principal principal = EasyMock.createMock(Principal.class);
        expect(principal.getName()).andReturn(principalName);
        expect(ctx.getUserPrincipal()).andReturn(principal);
        EasyMock.replay(ctx, principal);
        return ctx;
    }

    @BeforeEach
    public void setUp() {
        clusterState = EasyMock.createMock(ConnectClusterStateImpl.class);
        authorizer = EasyMock.createMock(ConnectAuthorizer.class);
        resource = new ConnectorPermissionsResource(clusterState, new ConnectorPermissionsService(authorizer));
    }

    @Test
    void noAuthorizer() {
        ConnectorPermissionsResource noopResource = new ConnectorPermissionsResource(clusterState, new ConnectorPermissionsService(authorizer));
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_RESOURCE_NAME));
        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andAnswer(() -> EasyMock.getCurrentArgument(1));
        EasyMock.replay(clusterState, authorizer);

        Response response = noopResource.listConnectorPermissions(mockSecurityContext("anonymous"));

        ConnectorPermissions permissions = unwrapEntity(response);
        assertEquals(new Permissions(true, true, true), permissions.getPermissions().get(TEST_RESOURCE_NAME));
        assertTrue(permissions.isCreateAllowed());
    }

    @Test
    void listConnectorPermissions_invalidPrincipal() {
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_RESOURCE_NAME));
        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andReturn(emptySet());
        EasyMock.replay(clusterState, authorizer);

        Response response = resource.listConnectorPermissions(mockSecurityContext("invalidPrincipal"));

        ConnectorPermissions permissions = unwrapEntity(response);
        assertTrue(permissions.getPermissions().isEmpty());
        assertFalse(permissions.isCreateAllowed());
    }

    @ParameterizedTest(name = "{index}. {0} can not access connector with name invalidResourceName.")
    @EnumSource(value = Operation.class)
    void listConnectorPermissions_invalidResourceName(Operation operation) {
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_RESOURCE_NAME));
        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andReturn(emptySet());
        EasyMock.replay(clusterState, authorizer);

        Response response = resource.listConnectorPermissions(mockSecurityContext(USER));

        ConnectorPermissions permissions = unwrapEntity(response);
        assertTrue(permissions.getPermissions().isEmpty());
    }

    @Test
    void listConnectorPermissions_viewerCanView() {
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_RESOURCE_NAME));
        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andReturn(Collections.singleton(new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.VIEW)));
        EasyMock.replay(clusterState, authorizer);

        Response response = resource.listConnectorPermissions(mockSecurityContext(USER));

        assertEquals(new Permissions(false, false, false), unwrapEntity(response).getPermissions().get(TEST_RESOURCE_NAME));
    }

    @ParameterizedTest(name = "{index}. user named {0} should not have permissions.")
    @EnumSource(value = Operation.class, names = {"DELETE", "EDIT"})
    void listConnectorPermissions_principalsWhoCanNotView(Operation operation) {
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_RESOURCE_NAME));
        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andReturn(Collections.singleton(new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), operation)));
        EasyMock.replay(clusterState, authorizer);

        Response response = resource.listConnectorPermissions(mockSecurityContext(USER));

        ConnectorPermissions permissions = unwrapEntity(response);
        assertNull(permissions.getPermissions().get(TEST_RESOURCE_NAME));
    }

    @Test
    void listConnectorPermissions_creatorWhoCanNotView() {
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_RESOURCE_NAME));
        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andReturn(Collections.singleton(new AuthorizableAction(Resource.connectorResource(TEST_RESOURCE_NAME), Operation.CREATE)));
        EasyMock.replay(clusterState, authorizer);

        Response response = resource.listConnectorPermissions(mockSecurityContext(USER));

        ConnectorPermissions permissions = unwrapEntity(response);
        assertNull(permissions.getPermissions().get(TEST_RESOURCE_NAME));
    }

    @ParameterizedTest(name = "{index}. user can view and have some privileges.")
    @MethodSource
    void listConnectorPermissions_success(boolean isCreateAllowed, Permissions expectedPermissions, Set<AuthorizableAction> allowedActions) {
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_RESOURCE_NAME));
        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andReturn(allowedActions);
        EasyMock.replay(clusterState, authorizer);

        Response response = resource.listConnectorPermissions(mockSecurityContext(USER));

        ConnectorPermissions actualPermissions = unwrapEntity(response);
        assertEquals(expectedPermissions, actualPermissions.getPermissions().get(TEST_RESOURCE_NAME));
    }

    @ParameterizedTest(name = "User is allowed to create: {0}.")
    @ValueSource(booleans = {true, false})
    void canCreate(boolean isCreateAllowed) {
        expect(clusterState.connectors()).andReturn(Collections.singletonList(TEST_RESOURCE_NAME));

        Set<AuthorizableAction> expectedActions = isCreateAllowed
            ? CONNECTOR_ACTIONS
            : difference(CONNECTOR_ACTIONS, Collections.singleton(CLUSTER_VALIDATE_ACTION));

        expect(authorizer.filterAuthorized(anyObject(), eq(CONNECTOR_ACTIONS))).andReturn(expectedActions);
        EasyMock.replay(clusterState, authorizer);

        Response response = resource.listConnectorPermissions(mockSecurityContext(USER));

        ConnectorPermissions actualPermissions = unwrapEntity(response);
        assertEquals(isCreateAllowed, actualPermissions.isCreateAllowed());
    }

    private static final <T> Set<T> difference(Set<? extends T> set1, Set<? extends T> set2) {
        HashSet<T> difference = new HashSet<>(set1);
        difference.removeAll(set2);
        return difference;
    }

}