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

import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Secret references look like this - mind that the UUID is present at a single place, without that references don't mean anything.
 * {
 *   "name": "MyConnector",
 *   "secret.properties": "password",
 *   "secret.bundle.id": "ABCD-EFGH-IJKL-MNOP",
 *   "password": "${secret:MyConnector/ABCD-EFGH-IJKL-MNOP:password}"
 * }
 */
public class SecretStorageExtension implements ConnectRestExtension {
    private static final Logger log = LoggerFactory.getLogger(ConnectSecretValidationFilter.class);

    public static final String VERSION = "1.0.0";
    public static final String SECRET_BUNDLE_ID = "secret.bundle.id";
    public static final String SENSITIVE_PROPERTY_LIST = "secret.properties";

    private boolean enabled;
    private String secretProviderAlias;
    private SecretStorage secretStorage;

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        if (!enabled) {
            return;
        }
        log.trace("Registering secret validation filter");
        restPluginContext.configurable().register(new ConnectSecretValidationFilter(secretStorage, secretProviderAlias));
        log.trace("Finished registering secret validation filter");

        log.trace("Registering secret management filter");
        restPluginContext.configurable().register(new ConnectSecretManagementFilter(secretStorage,
                restPluginContext.clusterState(), secretProviderAlias));
        log.trace("Finished registering secret management filter");
    }

    @Override
    public void close() {
        try {
            secretStorage.close();
        } catch (Exception e) {
            log.warn("Failed to close SecretStorage", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        configs = removeConfigProviderConfigs(configs);
        ConnectSecretExtensionConfig config = new ConnectSecretExtensionConfig(configs);
        enabled = config.isEnabled();
        if (!enabled) {
            log.info("Secret storage is disabled, skipping configuration");
        }
        secretProviderAlias = config.getSecretConfigProviderAlias();
        secretStorage = getStorage();
    }

    @Override
    public String version() {
        return VERSION;
    }

    //Visible for testing
    SecretStorage getStorage() {
        return SecretStorageSingleton.getOrCreate(null);
    }

    private Map<String, ?> removeConfigProviderConfigs(Map<String, ?> configs) {
        return configs
                .entrySet()
                .stream()
                .filter(e -> !e.getKey().startsWith(ConnectSecretStorageConfig.CONFIG_PROVIDERS_CONFIG))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
