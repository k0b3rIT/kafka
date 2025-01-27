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
// Copyright (c) 2023 Cloudera, Inc. All rights reserved.
package com.cloudera.kafka.security.ldap.internals;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
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
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.net.ServerSocket;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class LdapAuthenticationSSLTest extends AbstractLdapTestUnit {

    public static final String KEYSTORE_PASSWORD = "cloudera";
    private PlainSaslServer saslServer;
    private static int port;

    @BeforeAll
    public static void init() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        port = ss.getLocalPort();
        ss.close();

        setService(LdapAuthenticationTestUtil.setupDirectoryService());
        getService().startup();

        TcpTransport tcpTransport = new TcpTransport(port);
        tcpTransport.setEnableSSL(true);

        LdapServer ldapServer = LdapAuthenticationTestUtil.setupLdapServer(getService(), tcpTransport);

        ldapServer.setKeystoreFile(LdapAuthenticationTestUtil.getResourceUri("server.keystore"));
        ldapServer.setCertificatePassword(KEYSTORE_PASSWORD);

        setLdapServer(ldapServer);
        getLdapServer().start();
    }

    private static String getStoreTypeParameter(TestInfo info) {
        String[] storeTypes = {"jks", "pkcs12", "bks"};
        for (String storeType : storeTypes) {
            if (info.getDisplayName().contains("truststoreType=" + storeType)) {
                return storeType;
            }
        }
        throw new InvalidParameterException("The keystore type parameter must be one of jks, pkcs12, bks");
    }

    @BeforeEach
    public void setUp(TestInfo info) {
        String storeTypeParameter = getStoreTypeParameter(info);
        TestJaasConfig jaasConfig = new TestJaasConfig();
        Map<String, Object> options = new HashMap<>();
        options.put("ldap_url", "ldaps://localhost:" + port);
        options.put("user_dn_template", "uid={0},ou=users,ou=system");
        Map<String, Object> configs = new HashMap<>();

        configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, LdapAuthenticationTestUtil.getResourceUri(LdapAuthenticationTestUtil.getTruststoreResource(storeTypeParameter)));
        configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, new Password(KEYSTORE_PASSWORD));
        configs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, storeTypeParameter);
        configs.put(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL);
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), options);
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        PlainServerCallbackHandler callbackHandler = new LdapPlainServerCallbackHandler();
        callbackHandler.configure(configs, "PLAIN", jaasContext.configurationEntries());
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

    @ParameterizedTest(name = "truststoreType={0}")
    @ValueSource(strings = {"jks", "pkcs12", "bks"})
    public void testSuccessfulAuthentication(String truststoreType) {
        saslServer.evaluateResponse(LdapAuthenticationTestUtil.createSaslMessage(LdapAuthenticationTestUtil.USER_PASSWORD));
    }

    @ParameterizedTest(name = "truststoreType={0}")
    @ValueSource(strings = {"jks", "pkcs12", "bks"})
    public void testFailingAuthentication(String truststoreType) {
        assertThrows(
            SaslAuthenticationException.class,
            () -> saslServer.evaluateResponse(LdapAuthenticationTestUtil.createSaslMessage("incorrect_password"))
        );
    }

}
