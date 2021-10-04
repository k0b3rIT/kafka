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
import com.cloudera.kafka.connect.authorization.ResourceType;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectorPermissionsService {
    private static final Set<Operation> CONNECTORS_PAGE_OPERATIONS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(Operation.VIEW, Operation.DELETE, Operation.EDIT, Operation.MANAGE)));
    private static final AuthorizableAction CLUSTER_VALIDATE_ACTION = new AuthorizableAction(Resource.clusterResource(), Operation.VALIDATE, false, false);
    private final ConnectAuthorizer authorizer;

    public ConnectorPermissionsService(ConnectAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    public ConnectorPermissions fetchPermissionsResponse(Principal principal, Collection<String> connectorNames) {
        final Set<AuthorizableAction> permittedActions = fetchPermittedActions(principal, connectorNames);

        return new ConnectorPermissions(
            permittedActions.contains(CLUSTER_VALIDATE_ACTION),
            buildPermissionsForVisibleConnectors(permittedActions));

    }

    private Set<AuthorizableAction> fetchPermittedActions(Principal principal, Collection<String> connectorNames) {
        return authorizer.filterAuthorized(principal, buildEveryActionForConnectorsPage(connectorNames));
    }

    private static Map<String, Permissions> buildPermissionsForVisibleConnectors(Set<AuthorizableAction> actions) {
        final Map<String, Set<Operation>> allowedOperationsMap = buildAllowedOperationsMap(actions);

        return allowedOperationsMap.entrySet().stream()
                .filter(entry -> entry.getValue().contains(Operation.VIEW))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> createPermission(entry.getValue())
                ));
    }

    private static Map<String, Set<Operation>> buildAllowedOperationsMap(Set<AuthorizableAction> actions) {
        return actions.stream()
                .filter(a -> a.getResource().getResourceType().equals(ResourceType.CONNECTOR))
                .collect(Collectors.groupingBy(
                        action -> action.getResource().getResourceName(),
                        Collectors.mapping(
                                AuthorizableAction::getOperation,
                                Collectors.toSet())));
    }

    private static Permissions createPermission(Set<Operation> operations) {
        return new Permissions(
                operations.contains(Operation.EDIT),
                operations.contains(Operation.DELETE),
                operations.contains(Operation.MANAGE)
        );
    }

    private static Set<AuthorizableAction> buildEveryActionForConnectorsPage(Collection<String> connectorNames) {
        final Set<AuthorizableAction> actions = buildEveryConnectorLevelActionForConnectorsPage(connectorNames);
        actions.add(CLUSTER_VALIDATE_ACTION);
        return actions;
    }

    private static Set<AuthorizableAction> buildEveryConnectorLevelActionForConnectorsPage(Collection<String> connectorNames) {
        return connectorNames.stream()
                .map(Resource::connectorResource)
                .flatMap(ConnectorPermissionsService::buildActionsOnConnectorsPageForSingleConnector)
                .collect(Collectors.toCollection(() -> new HashSet<>()));
    }

    private static Stream<AuthorizableAction> buildActionsOnConnectorsPageForSingleConnector(Resource resource) {
        return CONNECTORS_PAGE_OPERATIONS.stream()
                .map(op -> new AuthorizableAction(resource, op, false, false));
    }
}
