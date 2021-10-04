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
package com.cloudera.kafka.connect.trustedproxy;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PrincipalValidator implements ConfigDef.Validator {
    private static final Pattern PRINCIPAL_REGEX =
            Pattern.compile("(?<primary>[^/\\s@]+)(/(?<instance>[\\w.-]+))?(@(?<realm>([^\\s]+)))?");

    private final boolean instanceRequired;
    private final boolean realmRequired;

    public PrincipalValidator(boolean instanceRequired, boolean realmRequired) {
        this.instanceRequired = instanceRequired;
        this.realmRequired = realmRequired;
    }

    public static PrincipalName parsePrincipal(String configName, String principal) {
        Matcher matcher = PRINCIPAL_REGEX.matcher(principal);
        if (!matcher.matches()) {
            throw new ConfigException(configName, principal, "Invalid principal");
        }
        String primary = matcher.group("primary");
        String instance = matcher.group("instance");
        String realm = matcher.group("realm");
        return new PrincipalName(primary, instance, realm);
    }

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            return;
        }

        if (!(value instanceof String)) {
            throw new ConfigException(name, value, "Value must be string");
        }

        String strVal = (String) value;
        PrincipalName principalName = parsePrincipal(name, strVal);
        if (instanceRequired && principalName.getInstance() == null) {
            throw new ConfigException(name, strVal, "Principal must contain the instance section");
        }
        if (realmRequired && principalName.getRealm() == null) {
            throw new ConfigException(name, strVal, "Principal must contain the realm section");
        }
    }
}
