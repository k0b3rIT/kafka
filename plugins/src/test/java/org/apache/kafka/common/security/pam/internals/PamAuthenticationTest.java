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

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.jvnet.libpam.PAM;
import org.jvnet.libpam.PAMException;
import org.jvnet.libpam.UnixUser;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class PamAuthenticationTest {

    private PlainSaslServer saslServer;
    private static final String USER_ID = "kafkaUser";
    private static final String USER_PASSWORD = "kafkaPassword";

    private class TestPAM extends PAM {

        TestPAM(String serviceName) throws PAMException {
            super(serviceName);
        }

        @Override
        public UnixUser authenticate(String username, String password) throws PAMException {
            if (username.equals(USER_ID) && password.equals(USER_PASSWORD)) {
                return null;
            } else {
                throw new PAMException("Not Authenticated");
            }
        }

    }

    private class TestPamPlainServerCallbackHandler extends PamPlainServerCallbackHandler {

        @Override
        protected PAM getPAM(String pamService) throws PAMException {
            return new TestPAM(pamService);
        }

    }

    @BeforeEach
    public void setUp() {
        saslServer = new PlainSaslServer(createAndConfigureCallbackHandler("login"));
    }

    @Test
    public void testSuccessfulAuthentication() {
        saslServer.evaluateResponse(saslMessage(USER_PASSWORD));
    }

    @Test
    public void testFailingAuthentication() {
        assertThrows(
            SaslAuthenticationException.class,
            () -> saslServer.evaluateResponse(saslMessage("incorrect_password"))
        );
    }

    @Test
    public void testFailingConfiguration() {
        assertThrows(
            IllegalStateException.class,
            () -> createAndConfigureCallbackHandler(null)
        );
    }

    private PlainServerCallbackHandler createAndConfigureCallbackHandler(String pamService) {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        Map<String, Object> options = new HashMap<>();
        options.put("pam_service", pamService);
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), options);
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        PlainServerCallbackHandler callbackHandler = new TestPamPlainServerCallbackHandler();
        callbackHandler.configure(null, "PLAIN", jaasContext.configurationEntries());

        return callbackHandler;
    }

    private byte[] saslMessage(String password) {
        String nul = "\u0000";
        String message = String.format("%s%s%s%s%s", USER_ID, nul, USER_ID, nul, password);
        return message.getBytes(StandardCharsets.UTF_8);
    }

}
