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
// Copyright (c) 2022 Cloudera, Inc. All rights reserved.
package com.cloudera.kafka.connect.common;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Mock implementation of RestServer interface for testing purpose.
 */
public class TestConnectRestServer {

    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 60 * 1000;

    private Server jettyServer;
    private ContextHandlerCollection handlers;

    public TestConnectRestServer() {
        jettyServer = new Server(0);
        handlers = new ContextHandlerCollection();

        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);
        jettyServer.setHandler(statsHandler);
        jettyServer.setStopTimeout(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
        jettyServer.setStopAtShutdown(true);
    }

    public void start() {
        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new ConnectException("Unable to initialize REST server", e);
        }
    }

    public void initializeResources(List<Object> resources) {
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new JacksonJsonProvider());
        resourceConfig.register(ConnectExceptionMapper.class);
        resourceConfig.property(ServerProperties.WADL_FEATURE_DISABLE, true);
        for (Object resource : resources) {
            resourceConfig.register(resource);
        }
        initResources(resourceConfig);
    }

    private void initResources(ResourceConfig resourceConfig) {
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);
        List<Handler> contextHandlers = new ArrayList<>();

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");
        contextHandlers.add(context);

        RequestLogHandler requestLogHandler = new RequestLogHandler();
        Slf4jRequestLogWriter slf4jRequestLogWriter = new Slf4jRequestLogWriter();
        slf4jRequestLogWriter.setLoggerName(RestServer.class.getCanonicalName());
        CustomRequestLog requestLog = new CustomRequestLog(slf4jRequestLogWriter, CustomRequestLog.EXTENDED_NCSA_FORMAT + " %msT");
        requestLogHandler.setRequestLog(requestLog);

        contextHandlers.add(new DefaultHandler());
        contextHandlers.add(requestLogHandler);

        handlers.setHandlers(contextHandlers.toArray(new Handler[0]));
        try {
            context.start();
        } catch (Exception e) {
            throw new ConnectException("Unable to initialize REST resources", e);
        }
    }

    public String serverBaseUrl() throws MalformedURLException {
        URL serverUrl = jettyServer.getURI().toURL();
        return "http://" + serverUrl.getHost() + ':' + serverUrl.getPort();
    }

    public void stop() {
        try {
            jettyServer.stop();
            jettyServer.join();
        } catch (Exception e) {
            jettyServer.destroy();
            throw new ConnectException("Unable to stop REST server", e);
        }
    }
}