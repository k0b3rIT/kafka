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
package org.apache.kafka.connect.mirror.rest;

import org.apache.kafka.connect.mirror.MirrorHerder;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.rest.resources.InternalMirrorResource;
import org.apache.kafka.connect.mirror.rest.resources.MirrorHerderResource;
import org.apache.kafka.connect.mirror.rest.resources.MirrorResource;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;

import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class MirrorRestServer extends RestServer {

    private final RestClient restClient;
    private Map<SourceAndTarget, MirrorHerder> herders;

    private Map<String, Object> props;

    public MirrorRestServer(Map<String, Object> props, RestClient restClient) {
        super(RestServerConfig.forPublic(10000, props));
        this.restClient = restClient;
        this.props = props;
    }

    public void initializeInternalResources(Map<SourceAndTarget, MirrorHerder> herders) {
        this.herders = herders;
        super.initializeResources();
    }

    @Override
    protected Collection<Class<?>> regularResources() {
        return Arrays.asList(
                InternalMirrorResource.class,
                MirrorHerderResource.class,
                MirrorResource.class
        );
    }

    @Override
    protected Collection<Class<?>> adminResources() {
        return Collections.emptyList();
    }

    @Override
    protected void configureRegularResources(ResourceConfig resourceConfig) {
        if (!herders.isEmpty()) {
            registerRestExtensions(herders.values().stream().collect(Collectors.toList()).get(0).plugins(), resourceConfig);
        }
        resourceConfig.register(new Binder());
    }

    private class Binder extends AbstractBinder {
        @Override
        protected void configure() {
            bind(herders).to(new TypeLiteral<Map<SourceAndTarget, MirrorHerder>>() { });
            bind(restClient).to(RestClient.class);
            bind(props).to(new TypeLiteral<Map<String, Object>>() { });
        }
    }

}
