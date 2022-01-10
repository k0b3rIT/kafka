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

package com.cloudera.kafka.connect.rest.authorization.extension.permissions;

import com.cloudera.kafka.connect.rest.authorization.extension.ConnectAuthorizerInstance;
import com.cloudera.kafka.connect.rest.authorization.extension.NoopAuthorizer;
import com.cloudera.kafka.connect.rest.authorization.extension.TestConnectAuthorizer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.cloudera.kafka.connect.rest.authorization.extension.ConnectSecurityConfig.KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG;
import static com.cloudera.kafka.connect.rest.authorization.extension.ConnectSecurityConfig.KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectPermissionsRestExtensionTest {

    @AfterEach
    void tearDown() {
        ConnectAuthorizerInstance.reset();
    }

    @Test
    void configure() {
        Map<String, Object> config = new HashMap<>();
        config.put(KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG, TestConnectAuthorizer.class.getName());
        config.put(KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_CONFIG, "superUser");
        // this shouldn't result in NoClassDefFoundError
        config.put("config.providers", "com.foo.bar.NonExistingConfigProvider");
        ConnectPermissionsRestExtension extension = new ConnectPermissionsRestExtension();
        extension.configure(config);
        assertTrue(extension.getAuthorizer() instanceof TestConnectAuthorizer);
    }

    @Test
    void configureMissingAuthorizer() {
        Map<String, Object> config = new HashMap<>();
        config.put(KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG, "");
        ConnectPermissionsRestExtension extension = new ConnectPermissionsRestExtension();
        extension.configure(config);
        assertTrue(extension.getAuthorizer() instanceof NoopAuthorizer);
    }
}