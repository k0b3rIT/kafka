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

import java.security.Principal;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.security.SpnegoUserIdentity;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.security.Credential;


public class TrustedProxyAuthorizationService implements AuthorizationService {

    private static final Credential NO_CREDENTIAL = new Credential() {
        @Override
        public boolean check(Object credentials) {
            return false;
        }
    };

    @Override
    public UserIdentity getUserIdentity(HttpServletRequest request, String name) {
        return createUserIdentity(name);
    }

    private UserIdentity createUserIdentity(String username) {
        Principal userPrincipal = new SpnegoUserPrincipal(username, "");
        Subject subject = new Subject();
        subject.getPrincipals().add(userPrincipal);
        subject.getPrivateCredentials().add(NO_CREDENTIAL);
        subject.setReadOnly();

        return new SpnegoUserIdentity(subject, userPrincipal, null);
    }

}
