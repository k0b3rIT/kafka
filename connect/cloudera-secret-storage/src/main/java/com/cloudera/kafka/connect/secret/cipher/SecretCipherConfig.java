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
package com.cloudera.kafka.connect.secret.cipher;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class SecretCipherConfig extends AbstractConfig {
    public static final String PBE_SALT_CONFIG = "kafka.connect.secret.pbe.salt";
    public static final String PBE_ITERATION_CONFIG = "kafka.connect.secret.pbe.iterations";
    public static final String GLOBAL_KEY_LOCATION_CONFIG = "kafka.connect.secret.global.key.location";
    public static final String GLOBAL_PASSWORD_CONFIG = "kafka.connect.secret.global.password";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PBE_SALT_CONFIG, ConfigDef.Type.STRING, NO_DEFAULT_VALUE,
                    lengthValidator(20), ConfigDef.Importance.HIGH, "Salt to be used during password based key derivation")
            .define(PBE_ITERATION_CONFIG, ConfigDef.Type.INT, 310000,
                    ConfigDef.Range.atLeast(300000), ConfigDef.Importance.MEDIUM, "Number of iterations used for password based key derivation")
            .define(GLOBAL_KEY_LOCATION_CONFIG, ConfigDef.Type.STRING, null,
                    existingPathValidator(), ConfigDef.Importance.HIGH,  "Location of global (wrapper) key")
            .define(GLOBAL_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "Global password");


    private static ConfigDef.Validator existingPathValidator() {
        return  ConfigDef.CompositeValidator.of(
                new ConfigDef.NonEmptyString(),
                ConfigDef.LambdaValidator.with(
                        (configName, configValue) -> {
                            String path = (String) configValue;
                            if (path == null) {
                                return;
                            }
                            Path globalKeyLocation = Paths.get(path);
                            if (!(Files.exists(globalKeyLocation)
                                    && Files.isDirectory(globalKeyLocation))
                                    && Files.isWritable(globalKeyLocation)) {
                                throw new ConfigException(configName, configValue, "Unreadable/nonexistent directory");
                            }
                        },
                        () -> "existing, readable directory of filesystem"
                )
        );
    }

    private static ConfigDef.Validator lengthValidator(int len) {
        return ConfigDef.CompositeValidator.of(
            new ConfigDef.NonEmptyString(),
            ConfigDef.LambdaValidator.with(
                    (configName, configValue) -> {
                        String str = (String) configValue;
                        byte[] representation = str.getBytes(StandardCharsets.UTF_8);
                        if (representation.length < len) {
                            throw new ConfigException(configName, configValue, "String does not represent enough entropy");
                        }
                    },
                    () -> "sufficiently long string"
            )
        );
    }


    public SecretCipherConfig(Map<String, ?> originals) {
        super(CONFIG_DEF, originals, true);
    }

    public Password getGlobalPassword() {
        return getPassword(GLOBAL_PASSWORD_CONFIG);
    }

    public String getGlobalKeyLocation() {
        String path = getString(GLOBAL_KEY_LOCATION_CONFIG);
        if (path == null) {
            return null;
        }
        Path globalKeyLocationDir = Paths.get(path);
        return globalKeyLocationDir.toAbsolutePath().toString();
    }

    public byte[] getPbeSalt() {
        return getString(PBE_SALT_CONFIG).getBytes(StandardCharsets.UTF_8);
    }

    public int getPbeIterations() {
        return getInt(PBE_ITERATION_CONFIG);
    }

}
