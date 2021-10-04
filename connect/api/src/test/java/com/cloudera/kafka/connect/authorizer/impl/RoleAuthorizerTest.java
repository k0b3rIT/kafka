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

package com.cloudera.kafka.connect.authorizer.impl;

import com.cloudera.kafka.connect.authorization.AuthorizableAction;
import com.cloudera.kafka.connect.authorization.Operation;
import com.cloudera.kafka.connect.authorization.Resource;
import com.cloudera.kafka.connect.authorization.impl.RoleAuthorizer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoleAuthorizerTest {
    private final RoleAuthorizer authorizer = new RoleAuthorizer();

    @ParameterizedTest(name = "User is authorized to do {0} Operation.")
    @EnumSource(Operation.class)
    void isAuthorized_authorized(Operation operation) {
        authorizer.configure(Collections.singletonMap(RoleAuthorizer.CONNECT_ADMINS_PROPERTY_KEY, "admin1,admin2"));

        assertTrue(authorizer.isAuthorized(new TestPrincipal("admin1"), buildAuthorizationInput(operation)));
    }

    @ParameterizedTest(name = "User is not authorized to do {0} Operation.")
    @EnumSource(Operation.class)
    void isAuthorized_notAuthorized(Operation operation) {
        authorizer.configure(Collections.singletonMap(RoleAuthorizer.CONNECT_ADMINS_PROPERTY_KEY, "admin1,admin2"));

        assertFalse(authorizer.isAuthorized(new TestPrincipal("anonymus"), buildAuthorizationInput(operation)));
    }

    @ParameterizedTest(name = "User with empty name is not authorized to do {0} Operation.")
    @EnumSource(Operation.class)
    void isAuthorized_emptyNameShouldNotBeAllowed(Operation operation) {
        authorizer.configure(Collections.singletonMap(RoleAuthorizer.CONNECT_ADMINS_PROPERTY_KEY, ""));

        assertFalse(authorizer.isAuthorized(new TestPrincipal(""), buildAuthorizationInput(operation)));
    }

    @ParameterizedTest(name = "Operator user is allowed to do action {0}: {1}")
    @MethodSource("operatorUserActions")
    void operatorUser_canViewAllAndManageConnectors(AuthorizableAction action, boolean allowed) {
        authorizer.configure(Collections.singletonMap(RoleAuthorizer.CONNECT_OPERATORS_PROPERTY_KEY, "operator"));
        assertEquals(allowed, authorizer.isAuthorized(new TestPrincipal("operator"), action));
    }

    @ParameterizedTest(name = "Viewer user is allowed to do action {0}: {1}")
    @MethodSource("viewerUserActions")
    void viewerUser_canViewAll(AuthorizableAction action, boolean allowed) {
        authorizer.configure(Collections.singletonMap(RoleAuthorizer.CONNECT_VIEWERS_PROPERTY_KEY, "operator"));
        assertEquals(allowed, authorizer.isAuthorized(new TestPrincipal("operator"), action));
    }

    private static Stream<Arguments> operatorUserActions() {
        Resource connector = Resource.connectorResource("connector1");
        return Stream.of(
            Arguments.of(new AuthorizableAction(connector, Operation.VIEW), true),
            Arguments.of(new AuthorizableAction(connector, Operation.MANAGE), true),
            Arguments.of(new AuthorizableAction(connector, Operation.CREATE), false),
            Arguments.of(new AuthorizableAction(connector, Operation.EDIT), false),
            Arguments.of(new AuthorizableAction(connector, Operation.DELETE), false),
            Arguments.of(new AuthorizableAction(Resource.clusterResource(), Operation.VIEW), true),
            Arguments.of(new AuthorizableAction(Resource.clusterResource(), Operation.VALIDATE), false),
            Arguments.of(new AuthorizableAction(Resource.clusterResource(), Operation.MANAGE), false)
        );
    }

    private static Stream<Arguments> viewerUserActions() {
        Resource connector = Resource.connectorResource("connector1");
        return Stream.of(
            Arguments.of(new AuthorizableAction(connector, Operation.VIEW), true),
            Arguments.of(new AuthorizableAction(connector, Operation.MANAGE), false),
            Arguments.of(new AuthorizableAction(connector, Operation.CREATE), false),
            Arguments.of(new AuthorizableAction(connector, Operation.EDIT), false),
            Arguments.of(new AuthorizableAction(connector, Operation.DELETE), false),
            Arguments.of(new AuthorizableAction(Resource.clusterResource(), Operation.VIEW), true),
            Arguments.of(new AuthorizableAction(Resource.clusterResource(), Operation.VALIDATE), false),
            Arguments.of(new AuthorizableAction(Resource.clusterResource(), Operation.MANAGE), false)
        );
    }


    @Test
    void isAuthorized_noProperty() {
        authorizer.configure(Collections.emptyMap());

        assertFalse(authorizer.isAuthorized(new TestPrincipal("admin2"), buildAuthorizationInput(Operation.CREATE)));
    }

    @Test
    void filterAuthorized_allAuthorized() {
        authorizer.configure(Collections.singletonMap(RoleAuthorizer.CONNECT_ADMINS_PROPERTY_KEY, "admin1,admin2"));

        Set<AuthorizableAction> authorized = authorizer.filterAuthorized(new TestPrincipal("admin2"), testActions());

        assertEquals(testActions(), authorized);
    }

    @Test
    void filterAuthorized_notAuthorized() {
        authorizer.configure(Collections.singletonMap(RoleAuthorizer.CONNECT_ADMINS_PROPERTY_KEY, "admin1,admin2"));

        Set<AuthorizableAction> authorized = authorizer.filterAuthorized(new TestPrincipal("anonymus"), testActions());

        assertTrue(authorized.isEmpty());
    }

    @Test
    void filterAuthorized_emptyNameShouldNotBeAllowed() {
        authorizer.configure(Collections.singletonMap(RoleAuthorizer.CONNECT_ADMINS_PROPERTY_KEY, ""));

        Set<AuthorizableAction> authorized = authorizer.filterAuthorized(new TestPrincipal(""), testActions());

        assertTrue(authorized.isEmpty());
    }

    @Test
    void filterAuthorized_noProperty() {
        authorizer.configure(Collections.emptyMap());

        Set<AuthorizableAction> authorized = authorizer.filterAuthorized(new TestPrincipal(""), testActions());

        assertTrue(authorized.isEmpty());
    }

    private Set<AuthorizableAction> testActions() {
        return Stream.of(
                        buildAuthorizationInput(Operation.CREATE),
                        buildAuthorizationInput(Operation.DELETE))
                .collect(Collectors.toSet());
    }

    private AuthorizableAction buildAuthorizationInput(Operation operation) {
        return new AuthorizableAction(
                Resource.connectorResource("testResourceName"),
                operation);
    }

    private static final class TestPrincipal implements Principal {
        private final String name;

        public TestPrincipal(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

}