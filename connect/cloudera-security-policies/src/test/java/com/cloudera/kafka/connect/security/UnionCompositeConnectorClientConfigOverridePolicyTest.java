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

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.sink.SinkConnector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnionCompositeConnectorClientConfigOverridePolicyTest {
    ConnectorClientConfigOverridePolicy mockPolicy1;
    ConnectorClientConfigOverridePolicy mockPolicy2;
    ConnectorClientConfigOverridePolicy mockPolicy3;
    List<ConnectorClientConfigOverridePolicy> mockPolicies;
    UnionCompositeConnectorClientConfigOverridePolicy compositePolicy;

    @BeforeEach
    public void setup() {
        mockPolicy1 = mock(ConnectorClientConfigOverridePolicy.class);
        mockPolicy2 = mock(ConnectorClientConfigOverridePolicy.class);
        mockPolicy3 = mock(ConnectorClientConfigOverridePolicy.class);
        mockPolicies = Arrays.asList(mockPolicy1, mockPolicy2, mockPolicy3);
        compositePolicy = new UnionCompositeConnectorClientConfigOverridePolicy(mockPolicies);
    }

    @Test
    public void testPolicyResultsAreMerged() {
        ConnectorClientConfigRequest request = new ConnectorClientConfigRequest(
                "testConnector",
                ConnectorType.SINK,
                SinkConnector.class,
                Collections.emptyMap(),
                ConnectorClientConfigRequest.ClientType.CONSUMER
        );

        expect(mockPolicy1.validate(eq(request))).andReturn(Collections.emptyList());
        ConfigValue policy2Value1 = new ConfigValue("some_config1");
        ConfigValue policy2Value2 = new ConfigValue("some_config2");
        expect(mockPolicy2.validate(eq(request))).andReturn(Arrays.asList(policy2Value1, policy2Value2));
        ConfigValue policy3Value1 = new ConfigValue("some_config3");
        expect(mockPolicy3.validate(eq(request))).andReturn(Collections.singletonList(policy3Value1));
        replay(mockPolicies.toArray());
        List<ConfigValue> expected = Arrays.asList(policy2Value1, policy2Value2, policy3Value1);

        List<ConfigValue> result = compositePolicy.validate(request);

        assertEquals(expected, result);
    }
}
