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

import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

public class LdapSSLSocketFactory extends SSLSocketFactory {
    private static final Logger log = LoggerFactory.getLogger(LdapSSLSocketFactory.class);
    private static LdapSSLSocketFactory ldapSSLSocketFactory;

    //This method called by the Java com.sun.jndi.ldap.Connection class through reflection
    public static synchronized SocketFactory getDefault() {
        return ldapSSLSocketFactory;
    }

    public static void init(Map<String, ?> configs) {
        try (DefaultSslEngineFactory defaultSslEngineFactory = new DefaultSslEngineFactory()) {
            defaultSslEngineFactory.configure(configs);
            ldapSSLSocketFactory = new LdapSSLSocketFactory(defaultSslEngineFactory.sslContext().getSocketFactory());
        }
    }

    private final SSLSocketFactory socketFactory;
    public LdapSSLSocketFactory(SSLSocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return socketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return socketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
        return socketFactory.createSocket(s, host, port, autoClose);
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return socketFactory.createSocket(host, port);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        return socketFactory.createSocket(host, port, localHost, localPort);
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        return socketFactory.createSocket(host, port);
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
        return socketFactory.createSocket(address, port, localAddress, localPort);
    }
}
