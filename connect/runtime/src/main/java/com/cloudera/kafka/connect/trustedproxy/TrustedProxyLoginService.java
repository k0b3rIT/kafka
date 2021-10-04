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

import java.nio.file.Path;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.security.ConfigurableSpnegoLoginService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SpnegoUserIdentity;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrustedProxyLoginService extends ContainerLifeCycle implements LoginService {

    private static final Logger LOG = LoggerFactory.getLogger(TrustedProxyLoginService.class);
    private final AuthorizationService authorizationService;
    private final ConfigurableSpnegoLoginService spnegoLoginService;
    private final Set<String> trustedProxies;

    private static final String DO_AS = "doAs";

    public TrustedProxyLoginService(String realm, AuthorizationService authService, List<String> trustedProxyList) {
        authorizationService = authService;
        spnegoLoginService = new ConfigurableSpnegoLoginService(realm, new TrustedProxyAuthorizationService());
        trustedProxies = new HashSet<>(trustedProxyList);
    }

    // ------- ConfigurableSpnegoLoginService methods -------

    /**
     * Sets the service name for spnego login.
     * @param serviceName the service name for spnego login
     */
    public void setServiceName(String serviceName) {
        spnegoLoginService.setServiceName(serviceName);
    }

    /**
     * Sets the hostname for spnego login.
     * @param hostName hostname for spnego login.
     */
    public void setHostName(String hostName) {
        spnegoLoginService.setHostName(hostName);
    }

    /**
     * Sets the keytab path for spnego login.
     * @param path keytab path for spnego login
     */
    public void setKeyTabPath(Path path) {
        spnegoLoginService.setKeyTabPath(path);
    }

    // ------- LoginService methods -------

    @Override
    public String getName() {
        return spnegoLoginService.getName();
    }

    @Override
    public UserIdentity login(String username, Object credentials, ServletRequest request) {
        SpnegoUserIdentity userIdentity = (SpnegoUserIdentity) spnegoLoginService.login(username, credentials, request);
        SpnegoUserPrincipal userPrincipal = (SpnegoUserPrincipal) userIdentity.getUserPrincipal();

        LOG.debug("User {} logged in", userPrincipal.getName());

        // Remove the host from the principal
        PrincipalName userPrincipalName = PrincipalValidator.parsePrincipal("", userPrincipal.getName());

        if (!trustedProxies.contains(userPrincipalName.getPrimary())) {
            LOG.debug("User {} is not authorized as a trusted proxy", userPrincipalName.getPrimary());
            return createUserIdentity(userPrincipalName.getPrimary(), request, userPrincipal);
        }

        String doAsUser = request.getParameter(DO_AS);
        if (doAsUser == null || doAsUser.isEmpty()) {
            LOG.debug("No doAs user was provided for the request");
            return createUserIdentity(userPrincipalName.getPrimary(), request, userPrincipal);
        } else {
            LOG.debug("Authenticating proxy user {} from {}", doAsUser, userPrincipalName.getPrimary());
            Principal principal = new TrustedProxyPrincipal(doAsUser, userPrincipal);
            Subject subject = new Subject(true, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());
            UserIdentity role = authorizationService.getUserIdentity((HttpServletRequest) request, doAsUser);
            return new SpnegoUserIdentity(subject, principal, role);
        }
    }

    @Override
    public boolean validate(UserIdentity user) {
        return spnegoLoginService.validate(user);
    }

    @Override
    public IdentityService getIdentityService() {
        return spnegoLoginService.getIdentityService();
    }

    @Override
    public void setIdentityService(IdentityService service) {
        spnegoLoginService.setIdentityService(service);
    }

    @Override
    public void logout(UserIdentity user) {
        spnegoLoginService.logout(user);
    }

    // ------- ContainerLifeCycle methods -------

    @Override
    protected void doStart() throws Exception {
        if (authorizationService instanceof LifeCycle) {
            ((LifeCycle) authorizationService).start();
        }
        spnegoLoginService.start();
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        spnegoLoginService.stop();
        if (authorizationService instanceof LifeCycle) {
            ((LifeCycle) authorizationService).stop();
        }
    }

    private UserIdentity createUserIdentity(String shortUserPrincipalName, ServletRequest request, SpnegoUserPrincipal userPrincipal) {
        Principal principal = new SpnegoUserPrincipal(shortUserPrincipalName, userPrincipal.getEncodedToken());
        Subject subject = new Subject(true, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());
        UserIdentity role = authorizationService.getUserIdentity((HttpServletRequest) request, userPrincipal.getName());
        return new SpnegoUserIdentity(subject, principal, role);
    }
}
