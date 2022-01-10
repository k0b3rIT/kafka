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

package com.cloudera.kafka.connect.rest.authorization.extension.permissions;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;
import com.cloudera.kafka.connect.rest.authorization.extension.ConnectAuthorizerInstance;
import com.cloudera.kafka.connect.rest.authorization.extension.ConnectSecurityConfig;

import java.io.IOException;
import java.util.Map;

public class ConnectPermissionsRestExtension implements ConnectRestExtension {

    private ConnectAuthorizer authorizer;

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        restPluginContext.configurable()
                .register(new ConnectorPermissionsResource(
                        restPluginContext.clusterState(),
                        new ConnectorPermissionsService(authorizer)));
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        ConnectSecurityConfig securityConfig = new ConnectSecurityConfig(configs);
        authorizer = ConnectAuthorizerInstance.getOrCreate(securityConfig);
    }

    /**
     * Visible for testing only.
     * @return the authorizer used by this extension
     */
    ConnectAuthorizer getAuthorizer() {
        return authorizer;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
