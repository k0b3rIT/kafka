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
package com.cloudera.kafka.connect.security;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UnionCompositePolicyConfig extends AbstractConfig {
    public static final String POLICY_LIST_CONFIG = "union.composite.policy.classes";
    private static final String POLICY_LIST_DOC = "List of ConnectorClientConfigOverridePolicy classes to apply.";
    private static final String UNION_POLICY_FULL_NAME = UnionCompositeConnectorClientConfigOverridePolicy.class.getName();
    private static final String UNION_POLICY_SIMPLE_NAME = UnionCompositeConnectorClientConfigOverridePolicy.class.getSimpleName();
    private static final String UNION_POLICY_PRUNED_NAME = UNION_POLICY_SIMPLE_NAME
            .substring(0, UNION_POLICY_SIMPLE_NAME.length() - ConnectorClientConfigOverridePolicy.class.getSimpleName().length());
    private static final Set<String> UNION_POLICY_ALIASES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            UNION_POLICY_FULL_NAME,
            UNION_POLICY_SIMPLE_NAME,
            UNION_POLICY_PRUNED_NAME
    )));

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    POLICY_LIST_CONFIG,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH,
                    POLICY_LIST_DOC
            );

    public UnionCompositePolicyConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public List<ConnectorClientConfigOverridePolicy> getPolicies() {
        Plugins plugins = new Plugins(originalsStrings());
        List<String> policyClassNames = new ArrayList<>(new LinkedHashSet<>(getList(POLICY_LIST_CONFIG)));
        return policyClassNames
                .stream()
                .filter(policyClassName -> !UNION_POLICY_ALIASES.contains(policyClassName))
                .map(policyClassName -> plugins.newPlugin(policyClassName, this, ConnectorClientConfigOverridePolicy.class))
                .collect(Collectors.toList());
    }
}
