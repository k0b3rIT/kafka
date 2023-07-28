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

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Provides the ability to authorize incoming requests.
 */
public class AuthorizationSecurityRestExtension implements ConnectRestExtension {

    private static final Logger LOG = LoggerFactory.getLogger(AuthorizationSecurityRestExtension.class);

    private ConnectAuthorizer authorizer;
    private Set<String> superUserPrincipalNames;

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        if (authorizer == null || authorizer instanceof NoopAuthorizer) {
            LOG.info("Connect authorization disabled.");
            return;
        }

        LOG.debug("Registering Cloudera auth request filter.");
        restPluginContext.configurable().register(
                new ConnectAuthorizationFilter(authorizer, restPluginContext.clusterState(), superUserPrincipalNames)
        );
        LOG.debug("Finished registering Cloudera auth request filter.");
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        ConnectSecurityConfig config = new ConnectSecurityConfig(configs);
        authorizer = ConnectAuthorizerInstance.getOrCreate(config);
        superUserPrincipalNames = config.getSuperUserPrincipalNames();
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Only for testing
     */
    ConnectAuthorizer getAuthorizer() {
        return authorizer;
    }
}
