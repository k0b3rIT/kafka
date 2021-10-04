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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class SpnegoConfig extends AbstractConfig {
    public static final String SPNEGO_ENABLED_CONFIG = "kafka.connect.spnego.enabled";
    public static final String SPNEGO_ENABLED_DOC = "Enables SPNEGO and Trusted Proxy in the Connect REST Server.";

    public static final String KAFKA_CONNECT_SERVICE_PRINCIPAL_CONFIG = "kafka.connect.spnego.service.principal";
    public static final String KAFKA_CONNECT_SERVICE_PRINCIPAL_DOC = "Service principal to be used by the Connect Worker with SPNEGO.";

    public static final String KAFKA_CONNECT_KEYTAB_LOCATION_CONFIG = "kafka.connect.spnego.keytab.path";
    public static final String KAFKA_CONNECT_KEYTAB_LOCATION_DOC = "Location of the keytab to be used by Connect Worker SPNEGO.";

    public static final String TRUSTED_PROXY_LIST_CONFIG = "kafka.connect.spnego.trusted.proxies";
    public static final String TRUSTED_PROXY_LIST_DOC = "List of trusted proxies to be accepted by the Connect REST Server.";

    public static final String KAFKA_CONNECT_CLIENT_PRINCIPAL_CONFIG = "kafka.connect.spnego.client.principal";
    public static final String KAFKA_CONNECT_CLIENT_PRINCIPAL_DOC = "Principal to be used by the Connect Worker as a client with SPNEGO.";

    public static final String RENEW_TGT_CONFIG = "kafka.connect.spnego.renew.tgt";
    public static final String RENEW_TGT_DOC = "Whether to renew TGT in the rest client of the Connect Worker.";

    public static final String USE_TICKET_CACHE_CONFIG = "kafka.connect.spnego.use.ticket.cache";
    public static final String USE_TICKET_CACHE_DOC = "Whether to use the ticket cache in the rest client of the Connect Worker.";

    public static final String TICKET_CACHE_PATH_CONFIG = "kafka.connect.spnego.ticket.cache.path";
    public static final String TICKET_CACHE_PATH_DOC = "Path of the ticket cache to be used by the Connect worker. Only used if " + USE_TICKET_CACHE_CONFIG + " is true.";

    protected static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SPNEGO_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false,
                    ConfigDef.Importance.MEDIUM, SPNEGO_ENABLED_DOC)
            .define(KAFKA_CONNECT_SERVICE_PRINCIPAL_CONFIG, ConfigDef.Type.STRING, null,
                    new PrincipalValidator(false, true), ConfigDef.Importance.MEDIUM,
                    KAFKA_CONNECT_SERVICE_PRINCIPAL_DOC)
            .define(KAFKA_CONNECT_KEYTAB_LOCATION_CONFIG, ConfigDef.Type.STRING, null,
                    new ReadableFileValidator(), ConfigDef.Importance.MEDIUM, KAFKA_CONNECT_KEYTAB_LOCATION_DOC)
            .define(TRUSTED_PROXY_LIST_CONFIG, ConfigDef.Type.LIST, "",
                    ConfigDef.Importance.MEDIUM, TRUSTED_PROXY_LIST_DOC)
            .define(KAFKA_CONNECT_CLIENT_PRINCIPAL_CONFIG, ConfigDef.Type.STRING, null,
                    new PrincipalValidator(false, false), ConfigDef.Importance.MEDIUM,
                    KAFKA_CONNECT_CLIENT_PRINCIPAL_DOC)
            .define(RENEW_TGT_CONFIG, ConfigDef.Type.BOOLEAN, false,
                    ConfigDef.Importance.LOW, RENEW_TGT_DOC)
            .define(USE_TICKET_CACHE_CONFIG, ConfigDef.Type.BOOLEAN, false,
                    ConfigDef.Importance.LOW, USE_TICKET_CACHE_DOC)
            .define(TICKET_CACHE_PATH_CONFIG, ConfigDef.Type.STRING, null,
                    new ReadableFileValidator(), ConfigDef.Importance.LOW, TICKET_CACHE_PATH_DOC);

    private final PrincipalName servicePrincipal;

    public SpnegoConfig(Map<String, ?> originals) {
        super(CONFIG_DEF, sanitizeConfig(originals), false);
        String principal = getString(KAFKA_CONNECT_SERVICE_PRINCIPAL_CONFIG);
        if (principal == null) {
            servicePrincipal = null;
        } else {
            servicePrincipal = PrincipalValidator.parsePrincipal(KAFKA_CONNECT_SERVICE_PRINCIPAL_CONFIG, principal);
        }
    }

    private static Map<String, ?> sanitizeConfig(Map<String, ?> originals) {
        return originals.entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .filter(e -> !CONFIG_PROVIDERS_CONFIG.equals(e.getKey()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public boolean isSpnegoEnabled() {
        return getBoolean(SPNEGO_ENABLED_CONFIG);
    }

    public boolean isServicePrincipalSet() {
        return getServicePrincipal() != null;
    }

    public PrincipalName getServicePrincipal() {
        return servicePrincipal;
    }

    public String getKeytabLocation() {
        return getString(KAFKA_CONNECT_KEYTAB_LOCATION_CONFIG);
    }

    public List<String> getTrustedProxies() {
        return getList(TRUSTED_PROXY_LIST_CONFIG);
    }

    public String getClientPrincipal() {
        return getString(KAFKA_CONNECT_CLIENT_PRINCIPAL_CONFIG);
    }

    public boolean isRenewTgt() {
        return getBoolean(RENEW_TGT_CONFIG);
    }

    public boolean isUseTicketCache() {
        return getBoolean(USE_TICKET_CACHE_CONFIG);
    }

    public String getTicketCachePath() {
        return getString(TICKET_CACHE_PATH_CONFIG);
    }

    public static class ReadableFileValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                return;
            }

            if (!(value instanceof String)) {
                throw new ConfigException(name, value, "Value must be string");
            }

            String strVal = (String) value;
            Path filePath = Paths.get(strVal);
            if (!Files.isReadable(filePath) || !Files.exists(filePath)) {
                throw new ConfigException(name, strVal, "File must exist and be readable");
            }
        }
    }

}
