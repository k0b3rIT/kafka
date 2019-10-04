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
// Copyright (c) 2019 Cloudera, Inc. All rights reserved.
package org.apache.kafka.common.security.pam.internals;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;

import org.jvnet.libpam.PAM;
import org.jvnet.libpam.PAMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

public class PamPlainServerCallbackHandler extends PlainServerCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(PamPlainServerCallbackHandler.class);
    private String pamService;

    @Override
    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.jaasConfigEntries = jaasConfigEntries;

        pamService = JaasContext.configEntryOption(jaasConfigEntries, "pam_service", PlainLoginModule.class.getName());

        if (pamService == null) {
            throw new IllegalStateException("pam_service is missing from the jaas conf file.");
        }
    }

    @Override
    protected boolean authenticate(String username, char[] password) {
        PAM pam = null;
        if (username != null) {
            try {
                pam = getPAM(pamService);
                pam.authenticate(username, new String(password));

                return true;
            } catch (PAMException e) {
                log.error("Authentication failed for user {}", username, e);
            } finally {
                if (pam != null) {
                    pam.dispose();
                }
            }
        }
        return false;
    }

    @Override
    public void close() throws KafkaException {

    }

    protected PAM getPAM(String pamService) throws PAMException {
        return new PAM(pamService);
    }

}