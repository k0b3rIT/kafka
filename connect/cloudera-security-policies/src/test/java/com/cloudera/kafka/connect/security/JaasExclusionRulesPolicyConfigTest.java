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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JaasExclusionRulesPolicyConfigTest {
    @Test
    public void testConfig() {
        Map<String, String> configs = new HashMap<>();
        configs.put(JaasExclusionRulesPolicyConfig.MODULE_LIST_CONFIG, "kerberos,plain");
        configs.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "kerberos.name", "KerberosLoginModule");
        configs.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "kerberos.exclude", "true");
        configs.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "plain.name", "PlainLoginModule");
        configs.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "plain.options.username", "some.*pattern.*");
        configs.put(JaasExclusionRulesPolicyConfig.MODULE_CONFIG_PREFIX + "plain.options.password", "a_different_pattern.*");

        JaasExclusionRulesPolicyConfig config = new JaasExclusionRulesPolicyConfig(configs);

        assertEquals(Arrays.asList("kerberos", "plain"), config.getModules());
        assertEquals("KerberosLoginModule", config.getModuleName("kerberos"));
        assertTrue(config.isModuleExcluded("kerberos"));
        assertEquals("PlainLoginModule", config.getModuleName("plain"));
        assertFalse(config.isModuleExcluded("plain"));
        Map<String, String> expectedPlainPropertyExcludes = new HashMap<>();
        expectedPlainPropertyExcludes.put("username", "some.*pattern.*");
        expectedPlainPropertyExcludes.put("password", "a_different_pattern.*");
        assertEquals(expectedPlainPropertyExcludes, config.getModuleOptionExcludes("plain"));
    }
}
