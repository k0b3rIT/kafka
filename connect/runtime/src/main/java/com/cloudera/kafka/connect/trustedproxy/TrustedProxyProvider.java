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

import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.authentication.ConfigurableSpnegoAuthenticator;
import org.eclipse.jetty.util.security.Constraint;

import java.nio.file.Paths;
import java.util.List;

public class TrustedProxyProvider {

    public static final String KAFKA_CONNECT_SERVICE_PRINCIPAL = "kafka.connect.spnego.service.principal";
    public static final String KAFKA_CONNECT_KEYTAB_LOCATION = "kafka.connect.spnego.keytab.path";
    public static final String TRUSTED_PROXY_LIST = "kafka.connect.spnego.trusted.proxies";
    public static final String SPNEGO_ENABLED = "kafka.connect.spnego.enabled";

    public static ConstraintSecurityHandler createSpnegoSecurityHandler(SpnegoConfig config) {
        if (!config.isServicePrincipalSet()) {
            throw new IllegalArgumentException(SpnegoConfig.KAFKA_CONNECT_SERVICE_PRINCIPAL_CONFIG + " must be set");
        }
        String keytabPath = config.getKeytabLocation();
        List<String> trustedProxies = config.getTrustedProxies();
        PrincipalName servicePrincipal = config.getServicePrincipal();

        // SPNEGO constraint
        Constraint constraint = new Constraint();
        constraint.setName(Constraint.__SPNEGO_AUTH);
        // Allow any authenticated user
        constraint.setRoles(new String[]{"**"});
        constraint.setAuthenticate(true);

        ConstraintMapping constraintMapping = new ConstraintMapping();
        constraintMapping.setConstraint(constraint);
        constraintMapping.setPathSpec("/*");

        // Authorization service
        TrustedProxyAuthorizationService authorizationService = new TrustedProxyAuthorizationService();
        TrustedProxyLoginService spnegoLoginService = new TrustedProxyLoginService(servicePrincipal.getRealm(),
                authorizationService, trustedProxies, config.getKerberosPrincipalToLocalRules());
        spnegoLoginService.addBean(authorizationService);
        spnegoLoginService.setKeyTabPath(Paths.get(keytabPath));
        spnegoLoginService.setServiceName(servicePrincipal.getPrimary());
        spnegoLoginService.setHostName(servicePrincipal.getInstance());

        // Security Handler
        ConstraintSecurityHandler trustedProxySecurityHandler = new ConstraintSecurityHandler();
        trustedProxySecurityHandler.setLoginService(spnegoLoginService);
        trustedProxySecurityHandler.setConstraintMappings(new ConstraintMapping[]{constraintMapping});
        trustedProxySecurityHandler.setAuthenticator(new ConfigurableSpnegoAuthenticator());

        return trustedProxySecurityHandler;
    }

}
