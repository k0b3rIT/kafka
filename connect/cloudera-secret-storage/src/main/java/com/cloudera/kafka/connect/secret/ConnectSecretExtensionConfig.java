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
package com.cloudera.kafka.connect.secret;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ConnectSecretExtensionConfig extends AbstractConfig {
    private static final String KAFKA_CONNECT_SECRET_CONFIG_PREFIX = "kafka.connect.secret.";

    public static final String STORAGE_ENABLED_CONFIG = KAFKA_CONNECT_SECRET_CONFIG_PREFIX + "enabled";
    public static final String STORAGE_ENABLED_DOC = "Whether to enable the Secret storage for sensitive configurations.";
    public static final boolean STORAGE_ENABLED_DEFAULT = true;

    public static final String SECRET_CONFIG_PROVIDER_ALIAS_CONFIG =
            KAFKA_CONNECT_SECRET_CONFIG_PREFIX + "provider.alias";
    public static final String SECRET_CONFIG_PROVIDER_ALIAS_DOC = "The alias of the config provider of secrets. Must be configured in the 'config.providers' list.";
    public static final String SECRET_CONFIG_PROVIDER_ALIAS_DEFAULT = "secret";


    private static final ConfigDef CONFIG = new ConfigDef()
            .define(
                    STORAGE_ENABLED_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    STORAGE_ENABLED_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    STORAGE_ENABLED_DOC
            )
            .define(
                    SECRET_CONFIG_PROVIDER_ALIAS_CONFIG,
                    ConfigDef.Type.STRING,
                    SECRET_CONFIG_PROVIDER_ALIAS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    SECRET_CONFIG_PROVIDER_ALIAS_DOC
            );

    public ConnectSecretExtensionConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    public boolean isEnabled() {
        return getBoolean(STORAGE_ENABLED_CONFIG);
    }

    public String getSecretConfigProviderAlias() {
        return getString(SECRET_CONFIG_PROVIDER_ALIAS_CONFIG);
    }
}
