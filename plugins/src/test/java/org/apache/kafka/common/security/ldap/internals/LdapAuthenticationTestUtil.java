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

import org.apache.commons.lang.SystemUtils;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.factory.DefaultDirectoryServiceFactory;
import org.apache.directory.server.i18n.I18n;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class LdapAuthenticationTestUtil {

    static final String USER_ID = "kafkaUser";
    static final String USER_PASSWORD = "ldappassword";

    private static final String LDIF = "dn: uid=kafkaUser,ou=users,ou=system\n" +
            "objectClass: uidObject\n" +
            "objectClass: person\n" +
            "objectClass: top\n" +
            "uid: kafkauUser\n" +
            "cn: Kafka User\n" +
            "sn: Kafka\n" +
            "userPassword: ldappassword";


    public static DirectoryService setupDirectoryService() throws Exception {
        DefaultDirectoryServiceFactory serviceFactory = new DefaultDirectoryServiceFactory();
        serviceFactory.init(UUID.randomUUID().toString());
        DirectoryService directoryService = serviceFactory.getDirectoryService();

        try (LdifReader reader = new LdifReader()) {
            List<LdifEntry> entries = reader.parseLdif(LDIF);
            for (LdifEntry entry : entries) {
                if (entry.isChangeAdd() || entry.isLdifContent()) {
                    directoryService.getAdminSession().add(new DefaultEntry(directoryService.getSchemaManager(), entry.getEntry()));
                } else if (entry.isChangeModify()) {
                    directoryService.getAdminSession().modify(entry.getDn(), entry.getModifications());
                } else {
                    throw new LdapException(I18n.err(I18n.ERR_117, entry.getChangeType()));
                }
            }
        }
        return directoryService;
    }

    public static LdapServer setupLdapServer(DirectoryService directoryService, TcpTransport tcpTransport) {
        LdapServer ldapServer = new LdapServer();
        ldapServer.setDirectoryService(directoryService);
        ldapServer.setTransports(tcpTransport);
        return ldapServer;
    }

    static String getResourceUri(String resourceName) {
        try {
            return LdapAuthenticationSSLTest.class.getClassLoader().getResource(resourceName).toURI().getPath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    static String getTruststoreResource(String keystoreType) {
        String javaVersion = "11";
        if (!SystemUtils.isJavaVersionAtLeast(1100)) {
            javaVersion = "8";
        }
        return "java" + javaVersion + "/truststore." + keystoreType;
    }

    static byte[] createSaslMessage(String password) {
        String nul = "\u0000";
        String message = String.format("%s%s%s%s%s", USER_ID, nul, USER_ID, nul, password);
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
