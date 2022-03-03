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

import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnionCompositePolicyConfigTest {
    @Test
    public void testClassLoading() {
        Map<String, String> props = new HashMap<>();
        props.put(
                UnionCompositePolicyConfig.POLICY_LIST_CONFIG,
                NoneConnectorClientConfigOverridePolicy.class.getName() + ",All"
        );
        UnionCompositePolicyConfig config = new UnionCompositePolicyConfig(props);

        List<ConnectorClientConfigOverridePolicy> policies = config.getPolicies();

        assertEquals(2, policies.size());
        assertTrue(policies.get(0) instanceof NoneConnectorClientConfigOverridePolicy);
        assertTrue(policies.get(1) instanceof AllConnectorClientConfigOverridePolicy);
    }
}
