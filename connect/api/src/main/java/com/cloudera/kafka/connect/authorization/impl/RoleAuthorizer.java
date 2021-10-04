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

package com.cloudera.kafka.connect.authorization.impl;

import org.apache.kafka.common.Configurable;

import com.cloudera.kafka.connect.authorization.AuthorizableAction;
import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;
import com.cloudera.kafka.connect.authorization.Operation;
import com.cloudera.kafka.connect.authorization.ResourceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public class RoleAuthorizer implements ConnectAuthorizer, Configurable {
    private static final Logger log = LoggerFactory.getLogger(RoleAuthorizer.class);
    public static final String CONNECT_ADMINS_PROPERTY_KEY = "kafka.connect.authorizer.role.admin.users";
    public static final String CONNECT_OPERATORS_PROPERTY_KEY = "kafka.connect.authorizer.role.operator.users";
    public static final String CONNECT_VIEWERS_PROPERTY_KEY = "kafka.connect.authorizer.role.viewer.users";

    private static final  Map<ResourceType, Set<Operation>> OPERATOR_ALLOWED_OPERATIONS;
    static {
        Map<ResourceType, Set<Operation>> map = new HashMap<>();
        map.put(ResourceType.CLUSTER, unmodifiableSet(EnumSet.of(Operation.VIEW)));
        map.put(ResourceType.CONNECTOR, unmodifiableSet(EnumSet.of(Operation.VIEW, Operation.MANAGE)));
        OPERATOR_ALLOWED_OPERATIONS =  unmodifiableMap(new EnumMap<>(map));
    }

    private Set<String> adminUsers;
    private Set<String> viewerUsers;
    private Set<String> operatorUsers;

    @Override
    public Set<AuthorizableAction> filterAuthorized(Principal principal, Set<AuthorizableAction> request) {
        if (adminUsers.contains(principal.getName())) {
            return request;
        } else if (operatorUsers.contains(principal.getName())) {
            return request.stream()
                .filter(RoleAuthorizer::isOperatorAction)
                .collect(Collectors.toSet());
        } else if (viewerUsers.contains(principal.getName())) {
            return request.stream()
                .filter(a -> a.getOperation().equals(Operation.VIEW))
                .collect(Collectors.toSet());
        }
        return emptySet();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        adminUsers = buildSet(configs.get(CONNECT_ADMINS_PROPERTY_KEY));
        operatorUsers = buildSet(configs.get(CONNECT_OPERATORS_PROPERTY_KEY));
        viewerUsers = buildSet(configs.get(CONNECT_VIEWERS_PROPERTY_KEY));

        if (adminUsers.isEmpty() && operatorUsers.isEmpty() && viewerUsers.isEmpty()) {
            log.warn("Role authorizer initialized with no users.");
        }
    }

    private static boolean isOperatorAction(AuthorizableAction action) {
        return OPERATOR_ALLOWED_OPERATIONS.getOrDefault(action.getResource().getResourceType(), Collections.<Operation>emptySet())
            .contains(action.getOperation());
    }

    private Set<String> buildSet(Object users) {
        return null == users
            ? emptySet()
            : Arrays.stream(users.toString().split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toSet());
    }
}