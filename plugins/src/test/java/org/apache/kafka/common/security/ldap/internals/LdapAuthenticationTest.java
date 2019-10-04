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
package org.apache.kafka.common.security.ldap.internals;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;

import org.apache.directory.api.util.FileUtils;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class LdapAuthenticationTest extends AbstractLdapTestUnit {

    private PlainSaslServer saslServer;

    @BeforeAll
    public static void init() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        ss.close();

        setService(LdapAuthenticationTestUtil.setupDirectoryService());
        getService().startup();

        LdapServer ldapServer = LdapAuthenticationTestUtil.setupLdapServer(getService(), new TcpTransport(port));
        setLdapServer(ldapServer);
        getLdapServer().start();
    }

    @BeforeEach
    public void setUp() {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        Map<String, Object> options = new HashMap<>();
        options.put("ldap_url", "ldap://localhost:" + getLdapServer().getPort());
        options.put("user_dn_template", "uid={0},ou=users,ou=system");
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), options);
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        PlainServerCallbackHandler callbackHandler = new LdapPlainServerCallbackHandler();
        callbackHandler.configure(Collections.emptyMap(), "PLAIN", jaasContext.configurationEntries());
        saslServer = new PlainSaslServer(callbackHandler);
    }

    @AfterAll
    public static void destroy() throws Exception {
        File instanceDirectory = getService().getInstanceLayout().getInstanceDirectory();
        getLdapServer().stop();
        getService().shutdown();
        setLdapServer(null);
        setService(null);
        FileUtils.deleteDirectory(instanceDirectory);
    }

    @Test
    public void testSuccessfulAuthentication() {
        saslServer.evaluateResponse(LdapAuthenticationTestUtil.createSaslMessage(LdapAuthenticationTestUtil.USER_PASSWORD));
    }

    @Test
    public void testFailingAuthentication() {
        assertThrows(
            SaslAuthenticationException.class,
            () -> saslServer.evaluateResponse(LdapAuthenticationTestUtil.createSaslMessage("incorrect_password"))
        );
    }

}
