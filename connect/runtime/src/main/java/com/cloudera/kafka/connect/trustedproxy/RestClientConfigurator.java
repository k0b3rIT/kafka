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

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.util.SPNEGOAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;

public class RestClientConfigurator {
    private static final Logger log = LoggerFactory.getLogger(RestClientConfigurator.class);

    /**
     * Configures the http client with additional configs.
     * @param client The client to configure.
     * @param url The URL the client will be used with.
     * @param workerConfig The worker config.
     * @return True if the AUTHORIZATION header can be forwarded from the original request, false otherwise.
     */
    public static boolean configureClient(HttpClient client, String url, AbstractConfig workerConfig) {
        SpnegoConfig config = new SpnegoConfig(workerConfig == null ? Collections.emptyMap() : workerConfig.originals());
        if (!config.isSpnegoEnabled()) {
            return true;
        }

        log.trace("Configuring HTTP client with SPNEGO");

        SPNEGOAuthentication authentication = new SPNEGOAuthentication(URI.create(url));
        authentication.setUserName(config.getClientPrincipal());
        authentication.setServiceName(config.getServicePrincipal().getPrimary());
        authentication.setUserKeyTabPath(Paths.get(config.getKeytabLocation()));
        authentication.setRenewTGT(config.isRenewTgt());
        authentication.setUseTicketCache(config.isUseTicketCache());
        if (config.isUseTicketCache()) {
            authentication.setTicketCachePath(Paths.get(config.getTicketCachePath()));
        }

        client.getAuthenticationStore().addAuthentication(authentication);
        return false;
    }
}
