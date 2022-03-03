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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasConfig;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.security.auth.login.AppConfigurationEntry;

/**
 * When configured, this policy check the sasl.jaas.config override and
 * - restricts usage of specific login modules
 * - restricts the values of specific properties in allowed login modules
 * If the override is not specified, nothing happens.
 */
public class JaasExclusionRulesConnectorClientConfigOverridePolicy implements ConnectorClientConfigOverridePolicy {
    private static final String JAAS_CONFIG_CONTEXT = "validation";
    private static final String ERROR_MESSAGE_TEMPLATE_INVALID_JAAS_CONFIG = "The JAAS configuration is invalid: %s";
    private static final String ERROR_MESSAGE_TEMPLATE_LOGIN_MODULE_NOT_ALLOWED =
            "The login module '%s' is not allowed in the JAAS configuration.";
    private static final String ERROR_MESSAGE_TEMPLATE_LOGIN_MODULE_OPTION_NOT_ALLOWED =
            "The value of option '%s' of login module '%s' is not allowed in the JAAS configuration.";

    private Set<String> excludedModules;
    private Map<String, Map<String, Pattern>> excludePatterns;

    public JaasExclusionRulesConnectorClientConfigOverridePolicy() {

    }

    // Visible for testing
    JaasExclusionRulesConnectorClientConfigOverridePolicy(Set<String> excludedModules, Map<String, Map<String, Pattern>> excludePatterns) {
        this.excludedModules = excludedModules;
        this.excludePatterns = excludePatterns;
    }

    @Override
    public List<ConfigValue> validate(ConnectorClientConfigRequest connectorClientConfigRequest) {
        Object jaasConfig = connectorClientConfigRequest.clientProps().get(SaslConfigs.SASL_JAAS_CONFIG);
        if (jaasConfig == null) {
            return Collections.emptyList();
        }
        String jaasConfigString = jaasConfig instanceof Password ? ((Password) jaasConfig).value() : jaasConfig.toString();

        AppConfigurationEntry[] entries;
        try {
            entries = parseJaasModuleEntries(jaasConfigString);
        } catch (IllegalArgumentException e) {
            return Collections.singletonList(
                    createSingleValueWithErrorMessage(jaasConfigString,
                            String.format(ERROR_MESSAGE_TEMPLATE_INVALID_JAAS_CONFIG, e.getMessage()))
            );
        }
        return Arrays
                .stream(entries)
                .map(entry -> validateEntry(entry, jaasConfigString))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        Set<String> excludedModules = new HashSet<>();
        Map<String, Map<String, Pattern>> excludePatterns = new HashMap<>();
        JaasExclusionRulesPolicyConfig config = new JaasExclusionRulesPolicyConfig(
                configs
                        .entrySet()
                        .stream()
                        .filter(e -> !e.getKey().startsWith(ConsumerConfig.CONFIG_PROVIDERS_CONFIG))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
        for (String module : config.getModules()) {
            String moduleName = config.getModuleName(module);
            if (config.isModuleExcluded(module)) {
                excludedModules.add(moduleName);
            } else {
                Map<String, Pattern> patterns = config
                        .getModuleOptionExcludes(module)
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> Pattern.compile(e.getValue())));
                excludePatterns.put(moduleName, patterns);
            }
        }

        this.excludedModules = Collections.unmodifiableSet(excludedModules);
        this.excludePatterns = Collections.unmodifiableMap(excludePatterns);
    }

    private List<ConfigValue> validateEntry(AppConfigurationEntry entry, String jaasConfig) {
        String moduleName = entry.getLoginModuleName();
        if (excludedModules.contains(moduleName)) {
            return Collections.singletonList(createExcludedModuleErrorMessage(moduleName, jaasConfig));
        }
        Map<String, Pattern> optionExclusions = excludePatterns.get(moduleName);
        if (optionExclusions == null) {
            return Collections.emptyList();
        }
        return entry
                .getOptions()
                .entrySet()
                .stream()
                .filter(e -> isOptionInvalid(e, optionExclusions))
                .map(e -> createExcludedModuleOptionErrorMessage(moduleName, e.getKey(), jaasConfig))
                .collect(Collectors.toList());
    }

    private boolean isOptionInvalid(Map.Entry<String, ?> option, Map<String, Pattern> optionExclusions) {
        Pattern excludePattern = optionExclusions.get(option.getKey());
        if (excludePattern == null) {
            return false;
        }
        String optionValue = (String) option.getValue();
        return excludePattern.matcher(optionValue).matches();
    }

    private ConfigValue createExcludedModuleErrorMessage(String moduleName, String jaasConfig) {
        return createSingleValueWithErrorMessage(
                jaasConfig,
                String.format(ERROR_MESSAGE_TEMPLATE_LOGIN_MODULE_NOT_ALLOWED, moduleName)
        );
    }

    private ConfigValue createExcludedModuleOptionErrorMessage(String moduleName, String optionName,
                                                               String jaasConfig) {
        return createSingleValueWithErrorMessage(
                jaasConfig,
                String.format(ERROR_MESSAGE_TEMPLATE_LOGIN_MODULE_OPTION_NOT_ALLOWED, optionName, moduleName)
        );
    }

    private ConfigValue createSingleValueWithErrorMessage(String jaasConfig, String errorMessage) {
        ConfigValue value = new ConfigValue(
                SaslConfigs.SASL_JAAS_CONFIG,
                jaasConfig,
                new ArrayList<>(),
                new ArrayList<>()
        );
        value.addErrorMessage(errorMessage);
        return value;
    }

    private AppConfigurationEntry[] parseJaasModuleEntries(String jaasConfig) {
        JaasConfig parsedConfig = new JaasConfig(JAAS_CONFIG_CONTEXT, jaasConfig);
        return parsedConfig.getAppConfigurationEntry(JAAS_CONFIG_CONTEXT);
    }
}
