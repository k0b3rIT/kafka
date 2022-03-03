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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.sink.SinkConnector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JaasConfigRequiredConnectorClientConfigOverridePolicyTest {
    JaasConfigRequiredConnectorClientConfigOverridePolicy policy;

    @BeforeEach
    public void setup() {
        policy = new JaasConfigRequiredConnectorClientConfigOverridePolicy();
    }

    @Test
    public void testJaasPresentIsOkWhenTypePassword() {
        List<ConfigValue> result = policy.validate(createRequest(new Password("some jaas config")));

        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testJaasPresentIsOkWhenTypeString() {
        List<ConfigValue> result = policy.validate(createRequest("some jaas config"));

        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testJaasMissingIsError() {
        List<ConfigValue> result = policy.validate(createRequest(null));

        assertEquals(1, result.size());
        ConfigValue value = result.get(0);
        assertEquals(SaslConfigs.SASL_JAAS_CONFIG, value.name());
        assertEquals(1, value.errorMessages().size());
    }

    private ConnectorClientConfigRequest createRequest(Object jaasConfig) {
        Map<String, Object> config = new HashMap<>();
        if (jaasConfig != null) {
            config.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        }
        return new ConnectorClientConfigRequest(
                "testConnector",
                ConnectorType.SINK,
                SinkConnector.class,
                config,
                ConnectorClientConfigRequest.ClientType.CONSUMER
        );
    }
}
