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
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.sink.SinkConnector;

import com.sun.security.auth.module.Krb5LoginModule;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * When configured, this policy forces Connectors to override the sasl.jaas.config property.
 */
public class JaasExclusionRulesConnectorClientConfigOverridePolicyTest {
    private static final String KERBEROS_LOGIN_MODULE = Krb5LoginModule.class.getName();
    private static final String SCRAM_LOGIN_MODULE = ScramLoginModule.class.getName();
    private static final String PLAIN_LOGIN_MODULE = PlainLoginModule.class.getName();

    @ParameterizedTest(name = "[{index}] - {0}")
    @MethodSource("policies")
    public void testNonExcludedModule(String ignored, JaasExclusionRulesConnectorClientConfigOverridePolicy policy) {
        ConnectorClientConfigRequest request = requestWithJaas(PLAIN_LOGIN_MODULE + " required username=\"kafka\";");

        List<ConfigValue> result = policy.validate(request);

        assertEquals(emptyList(), result);
    }

    @ParameterizedTest(name = "[{index}] - {0}")
    @MethodSource("policies")
    public void testNonExcludedModuleWithPasswordConfig(String ignored, JaasExclusionRulesConnectorClientConfigOverridePolicy policy) {
        ConnectorClientConfigRequest request = requestWithJaas(new Password(PLAIN_LOGIN_MODULE + " required valami=\"Test\" username=\"kafka\";"));

        List<ConfigValue> result = policy.validate(request);

        assertEquals(Collections.emptyList(), result);
    }

    @ParameterizedTest(name = "[{index}] - {0}")
    @MethodSource("policies")
    public void testExcludedModule(String ignored, JaasExclusionRulesConnectorClientConfigOverridePolicy policy) {
        ConnectorClientConfigRequest request = requestWithJaas(KERBEROS_LOGIN_MODULE + " required principal=\"kafka\";");

        List<ConfigValue> result = policy.validate(request);

        assertEquals(1, result.size());
        ConfigValue value = result.get(0);
        assertSingleErrorMessageContains(value, KERBEROS_LOGIN_MODULE);
    }

    @ParameterizedTest(name = "[{index}] - {0}")
    @MethodSource("policies")
    public void testOneExcludedProperty(String ignored, JaasExclusionRulesConnectorClientConfigOverridePolicy policy) {
        ConnectorClientConfigRequest request = requestWithJaas(SCRAM_LOGIN_MODULE + " required username=\"UserB\" password=\"1234\";");

        List<ConfigValue> result = policy.validate(request);

        assertEquals(1, result.size());
        ConfigValue value = result.get(0);
        assertSingleErrorMessageContains(value, "password");
    }

    @ParameterizedTest(name = "[{index}] - {0}")
    @MethodSource("policies")
    public void testMultipleExcludedProperties(String ignored, JaasExclusionRulesConnectorClientConfigOverridePolicy policy) {
        ConnectorClientConfigRequest request = requestWithJaas(SCRAM_LOGIN_MODULE + " required username=\"UserA\" password=\"1234\";");

        List<ConfigValue> result = policy.validate(request);

        assertEquals(2, result.size());
        result.forEach(value -> {
            assertEquals(SaslConfigs.SASL_JAAS_CONFIG, value.name());
            assertNotEquals(emptyList(), value.errorMessages());
        });
    }

    @ParameterizedTest(name = "[{index}] - {0}")
    @MethodSource("policies")
    public void testMultipleModules(String ignored, JaasExclusionRulesConnectorClientConfigOverridePolicy policy) {
        ConnectorClientConfigRequest request = requestWithJaas(
                SCRAM_LOGIN_MODULE + " required username=\"UserA\" password=\"1234\";"
                        + " " + KERBEROS_LOGIN_MODULE + " required principal=\"kafka\";"
                        + " " + PLAIN_LOGIN_MODULE + " required username=\"kafka\";"
        );

        List<ConfigValue> result = policy.validate(request);

        assertEquals(3, result.size());
        result.forEach(value -> {
            assertEquals(SaslConfigs.SASL_JAAS_CONFIG, value.name());
            assertNotEquals(emptyList(), value.errorMessages());
        });
    }

    @ParameterizedTest(name = "[{index}] - {0}")
    @MethodSource("policies")
    public void testInvalidSyntax(String ignored, JaasExclusionRulesConnectorClientConfigOverridePolicy policy) {
        ConnectorClientConfigRequest request = requestWithJaas(
                SCRAM_LOGIN_MODULE + " required username=\"UserA\" password=\"1234\";"
                        + " " + KERBEROS_LOGIN_MODULE + " required principal=\"kafka\"" // Closing semicolon is missing
        );

        List<ConfigValue> result = policy.validate(request);

        assertEquals(1, result.size());
    }

    @ParameterizedTest(name = "[{index}] - {0}")
    @MethodSource("policies")
    public void testUnknownFieldsAreValidInJaasConfig(String ignored, JaasExclusionRulesConnectorClientConfigOverridePolicy policy) {
        ConnectorClientConfigRequest request = requestWithJaas(
                new Password(SCRAM_LOGIN_MODULE + " required username=\"user\" password=\"pass\" testKey=\"testValue\";"));

        List<ConfigValue> result = policy.validate(request);

        assertEquals(Collections.emptyList(), result);
    }

    static Stream<Arguments> policies() {
        return Stream.of(
                Arguments.of("policy with ctor", createPolicyWithCtor()),
                Arguments.of("policy with props", createPolicyWithConfigure())
        );
    }

    private static JaasExclusionRulesConnectorClientConfigOverridePolicy createPolicyWithCtor() {
        return new JaasExclusionRulesConnectorClientConfigOverridePolicy(excludedKerberosModule(), excludedScramPropertyRules());
    }

    private static JaasExclusionRulesConnectorClientConfigOverridePolicy createPolicyWithConfigure() {
        Map<String, String> properties = new HashMap<>();
        properties.put(JaasExclusionRulesPolicyConfig.MODULE_LIST_CONFIG, "kerberos,scram");
        properties.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "kerberos.name", KERBEROS_LOGIN_MODULE);
        properties.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "kerberos.exclude", "true");
        properties.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "scram.name", SCRAM_LOGIN_MODULE);
        properties.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "scram.options.username", ".*[aA].*");
        properties.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "scram.options.password", ".*1.*");

        JaasExclusionRulesConnectorClientConfigOverridePolicy policy =
                new JaasExclusionRulesConnectorClientConfigOverridePolicy();
        policy.configure(properties);
        return policy;
    }

    private static Set<String> excludedKerberosModule() {
        return Collections.singleton(KERBEROS_LOGIN_MODULE);
    }

    private static Map<String, Map<String, Pattern>> excludedScramPropertyRules() {
        Map<String, Map<String, Pattern>> propertyExcludes = new HashMap<>();

        Map<String, Pattern> scramPropertyExcludes = new HashMap<>();
        // Do not allow usernames with 'a' or 'A' in it
        scramPropertyExcludes.put("username", Pattern.compile(".*[aA].*"));
        // Do not allow passwords with '1' in it
        scramPropertyExcludes.put("password", Pattern.compile(".*1.*"));
        propertyExcludes.put(SCRAM_LOGIN_MODULE, scramPropertyExcludes);

        return propertyExcludes;
    }

    private void assertSingleErrorMessageContains(ConfigValue value, String word) {
        assertEquals(SaslConfigs.SASL_JAAS_CONFIG, value.name());
        assertEquals(1, value.errorMessages().size());
        assertTrue(
                value.errorMessages().get(0).contains(word),
                "Error message should contain '" + word + "':" + value.errorMessages().get(0)
        );
    }

    private ConnectorClientConfigRequest requestWithJaas(Object jaasConfig) {
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
