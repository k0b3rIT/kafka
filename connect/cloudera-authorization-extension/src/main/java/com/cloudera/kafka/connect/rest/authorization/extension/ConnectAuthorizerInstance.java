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

import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectAuthorizerInstance {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectAuthorizerInstance.class);

    private static volatile ConnectAuthorizer authorizer;

    private ConnectAuthorizerInstance() {}

    public static ConnectAuthorizer getOrCreate(ConnectSecurityConfig config) {
        ConnectAuthorizer localAuthorizer = authorizer;
        if (localAuthorizer == null) {
            synchronized (ConnectAuthorizerInstance.class) {
                localAuthorizer = authorizer;
                if (localAuthorizer == null) {
                    LOG.info("Initializing connect authorizer.");
                    authorizer = localAuthorizer = initAuthorizer(config);
                } else {
                    LOG.info("Connect authorizer already initialized.");
                }
            }
        } else {
            LOG.info("Connect authorizer already initialized.");
        }
        return localAuthorizer;
    }

    /**
     * To be used in tests only.
     */
    public static void reset() {
        synchronized (ConnectAuthorizerInstance.class) {
            authorizer = null;
        }
    }

    private static ConnectAuthorizer initAuthorizer(ConnectSecurityConfig config) {
        try {
            return config.getAuthorizer();
        } catch (Exception e) {
            throw new ConfigException("Authorizer cannot created during configuration phase since the given class cannot instantiated", e);
        }
    }
}
