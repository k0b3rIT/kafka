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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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

    public static final String KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAMES_CONFIG =
            "kafka.connect.authorizer.super.user.principal.names";
    public static final String KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_DOC =
            "The super user principal names which will be exempt from authorization. " +
            "Only the super users can access internal Connect endpoints, so this must match the principal used by the" +
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
                    KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAMES_CONFIG,
                    Type.LIST,
                    emptyList(),
                    Importance.HIGH,
                    KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_DOC
            );

    public ConnectSecurityConfig(Map<String, ?> props) {
        super(CONFIG, sanitizeConfigs(props));
        if (isAuthorizerEnabled()) {
            Set<String> superUser = getSuperUserPrincipalNames();
            if (superUser == null || superUser.isEmpty()) {
                throw new ConfigException(
                        KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAMES_CONFIG,
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

    public Set<String> getSuperUserPrincipalNames() {
        List<String> usernames = getList(KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAMES_CONFIG);
        if (usernames.isEmpty() && originals().containsKey(KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_CONFIG)) {
            // Fallback on old config
            usernames = singletonList(originals()
                    .get(KAFKA_CONNECT_AUTHORIZER_SUPER_USER_PRINCIPAL_NAME_CONFIG).toString());
        }
        return Collections.unmodifiableSet(new HashSet<>(usernames));
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

}
