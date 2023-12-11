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

package com.cloudera.kafka.security.ldap.internals;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.ldap.DefaultLdapRealm;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.apache.shiro.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

public class LdapPlainServerCallbackHandler extends PlainServerCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(LdapPlainServerCallbackHandler.class);
    private DefaultSecurityManager securityManager;

    @Override
    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.jaasConfigEntries = jaasConfigEntries;
        @SuppressWarnings("unchecked")
        String ldapUrl = JaasContext.configEntryOption(jaasConfigEntries, "ldap_url", PlainLoginModule.class.getName());
        String userDnTemplate = JaasContext.configEntryOption(jaasConfigEntries, "user_dn_template", PlainLoginModule.class.getName());

        if (ldapUrl == null || userDnTemplate == null) {
            throw new IllegalStateException("ldap_url and/or user_dn_template is missing from the jaas conf file.");
        }
        DefaultLdapRealm realm = new DefaultLdapRealm();
        JndiLdapContextFactory contextFactory = (JndiLdapContextFactory) realm.getContextFactory();
        if (isSslEnabledForLdap(ldapUrl)) {
            if (configHasValidValue(configs, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) ||
                    configHasValidValue(configs, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)) {
                LdapSSLSocketFactory.init(configs);
                @SuppressWarnings("unchecked")
                Map<String, Object> env = (Map<String, Object>) contextFactory.getEnvironment();
                env.put("java.naming.ldap.factory.socket", "com.cloudera.kafka.security.ldap.internals.LdapSSLSocketFactory");
            }
        }
        realm.setUserDnTemplate(userDnTemplate);
        contextFactory.setUrl(ldapUrl);

        securityManager = new DefaultSecurityManager(realm);

    }

    private boolean isSslEnabledForLdap(String ldapUrl) {
        return StringUtils.startsWithIgnoreCase(ldapUrl, "ldaps");
    }

    private static boolean configHasValidValue(Map<String, ?> configs, String key) {
        return configs.containsKey(key) && configs.get(key) != null && !configs.get(key).toString().isEmpty();
    }

    @Override
    protected boolean authenticate(String username, char[] password) {
        if (username == null)
            return false;
        else {
            try {
                securityManager.authenticate(new UsernamePasswordToken(username, password));

                return true;
            } catch (AuthenticationException e) {
                log.error("Authentication failed for user {}", username, e);

                return false;
            }
        }
    }

}