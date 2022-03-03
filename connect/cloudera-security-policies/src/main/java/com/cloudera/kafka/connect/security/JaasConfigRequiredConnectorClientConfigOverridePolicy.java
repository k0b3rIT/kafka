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
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JaasConfigRequiredConnectorClientConfigOverridePolicy implements ConnectorClientConfigOverridePolicy {
    private static final String ERROR_MESSAGE_CONFIG_MUST_BE_OVERRIDDEN =
            "The configuration must be overridden and must not be empty.";

    @Override
    public List<ConfigValue> validate(ConnectorClientConfigRequest connectorClientConfigRequest) {
        Object jaasConfigObject = connectorClientConfigRequest.clientProps().get(SaslConfigs.SASL_JAAS_CONFIG);
        if (jaasConfigObject == null) {
            return createError("");
        }

        String jaasConfig = jaasConfigObject instanceof Password
            ? ((Password) jaasConfigObject).value()
            : jaasConfigObject.toString();
        if (jaasConfig.trim().isEmpty()) {
            return createError(jaasConfig);
        }
        return Collections.emptyList();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    private List<ConfigValue> createError(Object jaasConfig) {
        ConfigValue value = new ConfigValue(
                SaslConfigs.SASL_JAAS_CONFIG,
                jaasConfig,
                new ArrayList<>(),
                new ArrayList<>()
        );
        value.addErrorMessage(ERROR_MESSAGE_CONFIG_MUST_BE_OVERRIDDEN);
        return Collections.singletonList(value);
    }
}
