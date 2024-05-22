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
package com.cloudera.kafka.connect.trustedproxy;

import org.eclipse.jetty.security.ConfigurableSpnegoLoginService;
import org.eclipse.jetty.security.SpnegoUserIdentity;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.ietf.jgss.GSSContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TrustedProxyLoginServiceTest {
    private static final String REALM = "TEST_REALM";
    private static final String TRUSTED_PROXY1 = "proxy1";
    private static final String TRUSTED_PROXY2 = "proxy2foo";
    private static final List<String> TRUSTED_PROXIES =
            Collections.unmodifiableList(Arrays.asList(TRUSTED_PROXY1, TRUSTED_PROXY2));
    private static final List<String> ATL_RULES = Collections.singletonList("RULE:[1:$1@$0](.*@.*)s/@.*/foo/");
    @Mock
    AuthorizationService mockAuthorizationService;
    @Mock
    ConfigurableSpnegoLoginService mockLoginService;
    @Mock
    HttpServletRequest mockRequest;
    @Mock
    SpnegoUserIdentity mockAuthIdentity;
    @Mock
    UserIdentity mockRoleIdentity;
    @Mock
    SpnegoUserPrincipal mockPrincipal;
    @Mock
    UserIdentity.Scope mockScope;

    @Test
    public void testExtractSpnegoContext() throws ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        TrustedProxyLoginService service = new TrustedProxyLoginService(REALM, mockAuthorizationService,
                TRUSTED_PROXIES, Collections.emptyList(), mockLoginService);
        Class<?> contextClass =
                Class.forName("org.eclipse.jetty.security.ConfigurableSpnegoLoginService$SpnegoContext");
        Constructor<?> contextCtor = contextClass.getDeclaredConstructor();
        contextCtor.setAccessible(true);
        Object context = contextCtor.newInstance();
        Field contextField = ConfigurableSpnegoLoginService.class.getDeclaredField("_context");
        contextField.setAccessible(true);
        contextField.set(mockLoginService, context);

        service.extractSpnegoContext();
    }

    @Test
    public void testLoginWithoutKerberosRules() {
        setupMocks();

        String username = "user1";
        TrustedProxyLoginService service = createService(username, Collections.emptyList());
        when(mockPrincipal.getName()).thenReturn(username);
        Object credentials = new Object();

        UserIdentity userIdentity = service.login(username, credentials, mockRequest);

        assertEquals(username, userIdentity.getUserPrincipal().getName());
        verify(mockLoginService).login(eq(username), same(credentials), same(mockRequest));
        verify(mockAuthorizationService).getUserIdentity(same(mockRequest), eq(username));

        userIdentity.isUserInRole("ROLE", mockScope);
        verify(mockRoleIdentity).isUserInRole("ROLE", mockScope);
    }

    @Test
    public void testTrustedProxyWithoutKerberosRules() {
        setupMocks();

        String username = "user1";
        TrustedProxyLoginService service = createService(TRUSTED_PROXY1, Collections.emptyList());
        when(mockPrincipal.getName()).thenReturn(TRUSTED_PROXY1);
        when(mockRequest.getParameter("doAs")).thenReturn(username);
        Object credentials = new Object();

        UserIdentity userIdentity = service.login(TRUSTED_PROXY1, credentials, mockRequest);

        assertEquals(username, userIdentity.getUserPrincipal().getName());
        verify(mockLoginService).login(eq(TRUSTED_PROXY1), same(credentials), same(mockRequest));
        verify(mockAuthorizationService).getUserIdentity(same(mockRequest), eq(username));

        userIdentity.isUserInRole("ROLE", mockScope);
        verify(mockRoleIdentity).isUserInRole("ROLE", mockScope);
    }

    @Test
    public void testLoginWithKerberosRules() {
        setupMocks();

        String username = "user1";
        String principal = "user1@realm";
        String usernameReplaced = username + "foo";
        TrustedProxyLoginService service = createService(principal, ATL_RULES);
        when(mockPrincipal.getName()).thenReturn(principal);
        Object credentials = new Object();

        UserIdentity userIdentity = service.login(principal, credentials, mockRequest);

        assertEquals(usernameReplaced, userIdentity.getUserPrincipal().getName());
        verify(mockLoginService).login(eq(principal), same(credentials), same(mockRequest));
        verify(mockAuthorizationService).getUserIdentity(same(mockRequest), eq(usernameReplaced));

        userIdentity.isUserInRole("ROLE", mockScope);
        verify(mockRoleIdentity).isUserInRole("ROLE", mockScope);
    }

    @Test
    public void testTrustedProxyWithKerberosRules() {
        setupMocks();

        String username = "user1";
        String proxy = "proxy2@realm";
        TrustedProxyLoginService service = createService(proxy, ATL_RULES);
        when(mockPrincipal.getName()).thenReturn(proxy);
        when(mockRequest.getParameter("doAs")).thenReturn(username);
        Object credentials = new Object();

        UserIdentity userIdentity = service.login(proxy, credentials, mockRequest);

        assertEquals(username, userIdentity.getUserPrincipal().getName());
        verify(mockLoginService).login(eq(proxy), same(credentials), same(mockRequest));
        verify(mockAuthorizationService).getUserIdentity(same(mockRequest), eq(username));

        userIdentity.isUserInRole("ROLE", mockScope);
        verify(mockRoleIdentity).isUserInRole("ROLE", mockScope);
    }

    private TrustedProxyLoginService createService(String principalName, List<String> principalRules) {
        TrustedProxyLoginService service = spy(new TrustedProxyLoginService(REALM, mockAuthorizationService,
                TRUSTED_PROXIES, principalRules, mockLoginService));
        doReturn(mock(GSSContext.class)).when(service).addContext(any());
        doReturn(principalName).when(service).getFullPrincipalFromGssContext(any());
        return service;
    }

    private void setupMocks() {
        when(mockLoginService.login(any(), any(), any())).thenReturn(mockAuthIdentity);
        when(mockAuthIdentity.getUserPrincipal()).thenReturn(mockPrincipal);
        when(mockPrincipal.getEncodedToken()).thenReturn("TOKEN");
        when(mockAuthorizationService.getUserIdentity(any(), any())).thenReturn(mockRoleIdentity);
    }
}
