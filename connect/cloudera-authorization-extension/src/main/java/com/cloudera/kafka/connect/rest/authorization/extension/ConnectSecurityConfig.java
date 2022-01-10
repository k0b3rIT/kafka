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
// Copyright (c) 2021 Cloudera, Inc. All rights reserved.

package com.cloudera.kafka.connect.rest.authorization.extension;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;

import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

/**
 * ConnectSecurityConfig class.
 */
public class ConnectSecurityConfig extends AbstractConfig {
    public static final String KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG = "kafka.connect.authorizer.class.name";
    public static final String KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_DOC =
            "Defines the name of the class which will be used during user authorization.";
    public static final String KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_DEFAULT = NoopAuthorizer.class.getName();

    public static final String KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_CONFIG =
            "kafka.connect.authorizer.super.user.principal.name";
    public static final String KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_DOC =
            "The super user principal name which will be exempt from authorization. " +
            "Only the super user can access internal Connect endpoints, so this must match the principal used by the" +
            " Connect workers.";

    private static final ConfigDef CONFIG = new ConfigDef()
            .define(
                    KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG,
                    Type.CLASS,
                    KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_DEFAULT,
                    Importance.HIGH,
                    KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_DOC
            )
            .define(
                    KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_CONFIG,
                    Type.STRING,
                    null,
                    Importance.HIGH,
                    KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_DOC
            );

    public ConnectSecurityConfig(Map<String, ?> props) {
        super(CONFIG, sanitizeConfigs(props));
        if (isAuthorizerEnabled()) {
            String superUser = getSuperUserPrincipalName();
            if (superUser == null || superUser.isEmpty()) {
                throw new ConfigException(
                        KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_CONFIG,
                        superUser,
                        "Super user principal name must be provided when authorization is enabled."
                );
            }
        }
    }

    private static Map<String, ?> sanitizeConfigs(Map<String, ?> config) {
        return config.entrySet().stream()
            // These can cause trouble as may need plugin class loaders which are not available here
            .filter(e -> !CONFIG_PROVIDERS_CONFIG.equals(e.getKey()))
            // Filter out empty values for authorizer class name
            .filter(e -> !KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG.equals(e.getKey())
                || !(e.getValue() == null || ((String) e.getValue()).trim().isEmpty()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public boolean isAuthorizerEnabled() {
        Object className = originals().get(KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG);
        return className != null && !KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_DEFAULT.equals(className.toString());
    }

    public ConnectAuthorizer getAuthorizer() {
        return getConfiguredInstance(KAFKA_CONNECT_AUTHORIZER_CLASS_NAME_CONFIG, ConnectAuthorizer.class);
    }

    public String getSuperUserPrincipalName() {
        return getString(KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_CONFIG);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

}
