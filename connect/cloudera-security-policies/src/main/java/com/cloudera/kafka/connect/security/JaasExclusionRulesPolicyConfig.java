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
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Configuration for restricting JAAS login modules and their properties.
 * Modules can be excluded using: jaas.exclude.policy.module.[module_alias].exclude=true.
 * Module properties can be excluded using a regex: jaas.exclude.policy.module.[module_alias].options.[property_name]=kafka.*
 * Example config:
 * <pre>
 *     jaas.exclude.policy.modules=plain,kerberos
 *     # Exclude Krb5LoginModule fully
 *     jaas.exclude.policy.module.kerberos.name=com.sun.security.auth.module.Krb5LoginModule
 *     jaas.exclude.policy.module.kerberos.exclude=true
 *     # Exclude PlainLoginModule if the username starts with kafka
 *     jaas.exclude.policy.module.plain.name=org.apache.kafka.common.security.plain.PlainLoginModule
 *     jaas.exclude.policy.module.plain.options.username=kafka.*
 * </pre>
 */
public class JaasExclusionRulesPolicyConfig extends AbstractConfig {
    private static final String CONFIG_PREFIX = "jaas.exclude.policy.";
    static final String MODULE_CONFIG_PREFIX = CONFIG_PREFIX + "module.";
    private static final String EXCLUDE_SUFFIX = ".exclude";
    private static final String NAME_SUFFIX = ".name";
    private static final String OPTIONS_SUFFIX = ".options.";

    public static final String MODULE_LIST_CONFIG = CONFIG_PREFIX + "modules";
    private static final String MODULE_LIST_CONFIG_DOC = "List of login module aliases to exclude.";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    MODULE_LIST_CONFIG,
                    ConfigDef.Type.LIST,
                    "",
                    ConfigDef.Importance.HIGH,
                    MODULE_LIST_CONFIG_DOC
            );

    public JaasExclusionRulesPolicyConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    /**
     * @return Module aliases.
     */
    public List<String> getModules() {
        return getList(MODULE_LIST_CONFIG);
    }

    /**
     * @param module Module alias.
     * @return True if the module is excluded.
     */
    public boolean isModuleExcluded(String module) {
        String configName = MODULE_CONFIG_PREFIX + module + EXCLUDE_SUFFIX;
        Object exclude = originals().get(configName);
        if (exclude == null) {
            return false;
        }
        if (!(exclude instanceof String)) {
            throw new ConfigException(configName, exclude, "Must be a boolean.");
        }
        return Boolean.parseBoolean((String) exclude);
    }

    /**
     * @param module Module alias.
     * @return The fqdn of the module.
     */
    public String getModuleName(String module) {
        String configName = MODULE_CONFIG_PREFIX + module + NAME_SUFFIX;
        Object moduleName = originals().get(configName);
        if (!(moduleName instanceof String)) {
            throw new ConfigException(configName, moduleName, "Must be a string.");
        }
        return (String) moduleName;
    }

    /**
     * @param module Module alias
     * @return The property exclude patterns of the module in (property_key -> exclude_pattern) mappings.
     */
    public Map<String, String> getModuleOptionExcludes(String module) {
        String moduleOptionsPrefix = MODULE_CONFIG_PREFIX + module + OPTIONS_SUFFIX;
        return originals()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(moduleOptionsPrefix))
                .collect(Collectors.toMap(
                        e -> e.getKey().substring(moduleOptionsPrefix.length()),
                        e -> (String) e.getValue()
                ));
    }
}
