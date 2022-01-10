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

import com.cloudera.kafka.connect.secret.store.KafkaSecretStorage;

import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Configs of {@link SecretStorage} instantiation. Used by {@link SecretConfigProvider}.
 * Can resolve configuration values from the environment by using the {@link #ENV_SOURCE_POSTFIX} postfix in the property key and setting the value to the env var name to fetch the value from.
 */
public class ConnectSecretStorageConfig extends AbstractConfig {
    private static final String KAFKA_CONNECT_SECRET_CONFIG_PREFIX = "kafka.connect.secret.";
    public static final String KAFKA_CONNECT_SECRET_STORAGE_CLASS_NAME_CONFIG =
            KAFKA_CONNECT_SECRET_CONFIG_PREFIX + "storage.class.name";
    public static final String KAFKA_CONNECT_SECRET_STORAGE_CLASS_NAME_DOC =
            "Defines the name of the class which will be used to store Kafka Connect configuration secrets.";
    public static final String KAFKA_CONNECT_SECRET_STORAGE_CLASS_NAME_DEFAULT = KafkaSecretStorage.class.getName();
    static final String ENV_SOURCE_POSTFIX = ".source.env";

    private static final ConfigDef CONFIG = new ConfigDef()
            .define(
                    KAFKA_CONNECT_SECRET_STORAGE_CLASS_NAME_CONFIG,
                    ConfigDef.Type.CLASS,
                    KAFKA_CONNECT_SECRET_STORAGE_CLASS_NAME_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    KAFKA_CONNECT_SECRET_STORAGE_CLASS_NAME_DOC
            );

    public ConnectSecretStorageConfig(Map<String, ?> props) {
        super(CONFIG, processProps(props));
    }

    //Visible for testing
    static Map<String, ?> processProps(Map<String, ?> props, Function<String, String> envVarSource) {
        return props.entrySet()
                .stream()
                .map(e -> {
                    if (!e.getKey().endsWith(ENV_SOURCE_POSTFIX)) {
                        return e;
                    }
                    return new AbstractMap.SimpleImmutableEntry<>(
                            removePostfix(e.getKey(), ENV_SOURCE_POSTFIX),
                            getFromEnv(e.getKey(), e.getValue(), envVarSource)
                    );
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, ?> processProps(Map<String, ?> props) {
        return processProps(props, System::getenv);
    }

    private static String removePostfix(String value, String postfix) {
        return value.substring(0, value.length() - postfix.length());
    }

    private static Object getFromEnv(String propertyKey, Object envVarName, Function<String, String> envVarSource) {
        if (!(envVarName instanceof String)) {
            throw new RuntimeException("Value of properties ending with " + ENV_SOURCE_POSTFIX
                    + " must be of type string. Key " + propertyKey + " has invalid value.");
        }
        return envVarSource.apply((String) envVarName);
    }

    public SecretStorage getSecretStorage() {
        return getConfiguredInstance(KAFKA_CONNECT_SECRET_STORAGE_CLASS_NAME_CONFIG, SecretStorage.class);
    }
}
