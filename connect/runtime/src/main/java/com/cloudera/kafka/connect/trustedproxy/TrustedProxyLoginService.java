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

import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;

import org.eclipse.jetty.security.ConfigurableSpnegoLoginService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SpnegoUserIdentity;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

public class TrustedProxyLoginService extends ContainerLifeCycle implements LoginService {

    private static final Logger LOG = LoggerFactory.getLogger(TrustedProxyLoginService.class);
    private static final String GSS_HOLDER_CLASS_NAME =
            "org.eclipse.jetty.security.ConfigurableSpnegoLoginService$GSSContextHolder";
    private static final String REQUEST_ATTR_ADDED_NEW_SESSION =
            TrustedProxyLoginService.class.getName() + "#ADDED_NEW_SESSION";
    private final GSSManager gssManager = GSSManager.getInstance();
    private final AuthorizationService authorizationService;
    private final ConfigurableSpnegoLoginService spnegoLoginService;
    private final Set<String> trustedProxies;
    private final KerberosShortNamer kerberosShortNamer;
    private Subject spnegoSubject;
    private GSSCredential spnegoServiceCredential;
    private Constructor<?> holderConstructor;

    private static final String DO_AS = "doAs";

    public TrustedProxyLoginService(String realm, AuthorizationService authService, List<String> trustedProxyList,
                                    List<String> principalToLocalRules) {
        this(realm, authService, trustedProxyList, principalToLocalRules,
                new ConfigurableSpnegoLoginService(realm, new TrustedProxyAuthorizationService()));
    }

    // Visible for testing
    TrustedProxyLoginService(String realm, AuthorizationService authService, List<String> trustedProxyList,
                             List<String> principalToLocalRules, ConfigurableSpnegoLoginService loginService) {
        authorizationService = authService;
        spnegoLoginService = loginService;
        trustedProxies = new HashSet<>(trustedProxyList);
        kerberosShortNamer = principalToLocalRules == null || principalToLocalRules.isEmpty()
                ? null
                : KerberosShortNamer.fromUnparsedRules(realm, principalToLocalRules);
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
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        GSSContext gssContext = addContext(httpRequest);
        SpnegoUserIdentity userIdentity = (SpnegoUserIdentity) spnegoLoginService.login(username, credentials, request);
        SpnegoUserPrincipal userPrincipal = (SpnegoUserPrincipal) userIdentity.getUserPrincipal();
        String fullPrincipal = getFullPrincipalFromGssContext(gssContext);
        cleanRequest(httpRequest);
        LOG.debug("User {} logged in with full principal {}", userPrincipal.getName(), fullPrincipal);

        // Remove the host from the principal
        PrincipalName userPrincipalName = PrincipalValidator.parsePrincipal("", fullPrincipal);
        String userShortname = userPrincipalName.getPrimary();
        if (kerberosShortNamer != null) {
            try {
                userShortname = kerberosShortNamer.shortName(new KerberosName(userPrincipalName.getPrimary(),
                        userPrincipalName.getInstance(), userPrincipalName.getRealm()));
                LOG.debug("Principal {} was shortened to {}", userPrincipalName, userShortname);
                userPrincipal = new SpnegoUserPrincipal(userShortname, userPrincipal.getEncodedToken());
            } catch (IOException e) {
                throw new RuntimeException("Could not generate short name for principal " + userPrincipalName, e);
            }
        }

        if (!trustedProxies.contains(userShortname)) {
            LOG.debug("User {} is not authorized as a trusted proxy", userShortname);
            return createUserIdentity(userShortname, request, userPrincipal);
        }

        String doAsUser = request.getParameter(DO_AS);
        if (doAsUser == null || doAsUser.isEmpty()) {
            LOG.debug("No doAs user was provided for the request");
            return createUserIdentity(userShortname, request, userPrincipal);
        } else {
            LOG.debug("Authenticating proxy user {} from {}", doAsUser, userShortname);
            Principal principal = new TrustedProxyPrincipal(doAsUser, userPrincipal);
            Subject subject = new Subject(true, Collections.singleton(principal), Collections.emptySet(),
                    Collections.emptySet());
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
        extractSpnegoContext();
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

    // Visible for testing
    String getFullPrincipalFromGssContext(GSSContext gssContext) {
        try {
            return gssContext.getSrcName().toString();
        } catch (GSSException e) {
            throw new RuntimeException("Failed to extract full principal", e);
        }
    }

    // Visible for testing
    void extractSpnegoContext() {
        // All of the following code depends on the structure of
        // org.eclipse.jetty.security.ConfigurableSpnegoLoginService$GSSContextHolder and
        // org.eclipse.jetty.security.ConfigurableSpnegoLoginService$SpnegoContext
        // If jetty is upgraded, this code might brake
        try {
            Field contextField = ConfigurableSpnegoLoginService.class.getDeclaredField("_context");
            contextField.setAccessible(true);
            Object spnegoContext = contextField.get(spnegoLoginService);
            Class<?> contextClass = spnegoContext.getClass();
            Field contextSubjectField = contextClass.getDeclaredField("_subject");
            contextSubjectField.setAccessible(true);
            spnegoSubject = (Subject) contextSubjectField.get(spnegoContext);
            Field contextCredentialField = contextClass.getDeclaredField("_serviceCredential");
            contextCredentialField.setAccessible(true);
            spnegoServiceCredential = (GSSCredential) contextCredentialField.get(spnegoContext);
            Class<?> gssHolder = Class.forName(GSS_HOLDER_CLASS_NAME);
            holderConstructor = gssHolder.getDeclaredConstructor(GSSContext.class);
            holderConstructor.setAccessible(true);
        } catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to init SPNEGO context", e);
        }

    }

    // Visible for testing
    GSSContext addContext(HttpServletRequest request) {
        // This tries to inject an externally created GSSContext into ConfigurableSpnegoLoginService
        // ConfigurableSpnegoLoginService drops the realm part of the client principal, but we need that to be able to
        // evaluate the auth to local rules
        // By injecting the GSSContext through the session, we can get access to the full principal
        // If jetty is upgraded, this code might brake
        try {
            GSSContext gssContext = Subject.doAs(spnegoSubject, newGSSContext(spnegoServiceCredential));
            Object holder = holderConstructor.newInstance(gssContext);
            boolean needsNewSession = request.getSession(false) == null;
            if (needsNewSession) {
                request.setAttribute(REQUEST_ATTR_ADDED_NEW_SESSION, "true");
            }
            request.getSession(true).setAttribute(GSS_HOLDER_CLASS_NAME, holder);
            return gssContext;
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Failed to perform SPNEGO authentication", e);
        }
    }

    private void cleanRequest(HttpServletRequest request) {
        if (!"true".equals(request.getAttribute(REQUEST_ATTR_ADDED_NEW_SESSION))) {
            return;
        }
        request.removeAttribute(REQUEST_ATTR_ADDED_NEW_SESSION);
        HttpSession session = request.getSession();
        if (session != null) {
            try {
                session.invalidate();
            } catch (Exception e) {
                //NOP
            }
        }
    }

    private PrivilegedAction<GSSContext> newGSSContext(GSSCredential serviceCredential) {
        return () -> {
            try {
                return gssManager.createContext(serviceCredential);
            } catch (GSSException x) {
                throw new RuntimeException(x);
            }
        };
    }

    private UserIdentity createUserIdentity(String shortUserPrincipalName, ServletRequest request, SpnegoUserPrincipal userPrincipal) {
        Principal principal = new SpnegoUserPrincipal(shortUserPrincipalName, userPrincipal.getEncodedToken());
        Subject subject = new Subject(true, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());
        UserIdentity role = authorizationService.getUserIdentity((HttpServletRequest) request, shortUserPrincipalName);
        return new SpnegoUserIdentity(subject, principal, role);
    }
}
