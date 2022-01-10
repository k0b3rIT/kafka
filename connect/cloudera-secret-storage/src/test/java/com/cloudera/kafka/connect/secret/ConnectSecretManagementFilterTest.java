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

package com.cloudera.kafka.connect.secret;

import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.easymock.Capture;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.UriInfo;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ConnectSecretManagementFilterTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testGetSecretConfigs() {
        Set<String> secretKeys = new HashSet<>();
        secretKeys.add("prop1");
        secretKeys.add("prop2");

        Map<String, String> configs = new HashMap<>();
        configs.put("prop1", "val1");
        configs.put("prop2", "val2");
        configs.put("prop3", "val3");
        configs.put(
                SecretStorageExtension.SENSITIVE_PROPERTY_LIST,
                String.join(",", secretKeys));

        Map<String, String> secretConfigs = ConnectSecretManagementFilter.getSecretConfigs(configs);
        assertEquals(secretKeys, secretConfigs.keySet());
    }

    @Test
    public void testRedactSecretConfigs() {
        Set<String> secretKeys = new HashSet<>();
        secretKeys.add("prop1");
        secretKeys.add("prop2");

        Map<String, String> configs = new HashMap<>();
        configs.put("prop1", "val1");
        configs.put("prop2", "val2");
        configs.put("prop3", "val3");
        configs.put(
                SecretStorageExtension.SENSITIVE_PROPERTY_LIST,
                String.join(",", secretKeys));
        String connectorName = "testRedactSecretConfigs";
        String bundleId = "UUID";
        Map<String, String> redactedConfigs =
                ConnectSecretManagementFilter.redactSecretConfigs("secret", connectorName, bundleId, configs);

        Map<String, String> expectedRedactedConfigs = new HashMap<>(configs);
        expectedRedactedConfigs.put("prop1", "${secret:testRedactSecretConfigs/UUID:prop1}");
        expectedRedactedConfigs.put("prop2", "${secret:testRedactSecretConfigs/UUID:prop2}");

        assertEquals(expectedRedactedConfigs, redactedConfigs);
        assertEquals(configs.size(), redactedConfigs.size());
    }

    @Test
    public void testParseReference() {
        BundlePath path = new BundlePath("testParseReference", "UUID");
        String prop = "prop1:some_special_chars";
        String referenceString = "${secret:testParseReference/UUID:prop1:some_special_chars}";
        ConnectSecretManagementFilter.SecretStorageReference ref =
                ConnectSecretManagementFilter.SecretStorageReference.readReference(referenceString, "secret");
        assertEquals("secret", ref.getConfigProvider());
        assertEquals(path, ref.getPath());
        assertEquals(prop, ref.getProperty());
    }

    @Test
    public void testParseCreateRequestContent() throws IOException {
        String connectorName = "testParseCreateRequestContent";

        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        expect(requestContext.getMethod()).andReturn(HttpMethod.POST);
        UriInfo uriInfo = mock(UriInfo.class);
        expect(uriInfo.getPath()).andReturn("connectors/").anyTimes();
        expect(requestContext.getUriInfo()).andReturn(uriInfo).anyTimes();

        Map<String, String> config = new HashMap<>();
        config.put("prop1", "va1");
        config.put("prop2", "va2");
        CreateConnectorRequest request = new CreateConnectorRequest(connectorName, config, null);

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> requestBody = objectMapper.convertValue(request, new TypeReference<Map<String, Object>>() {
        });
        InputStream in = new ByteArrayInputStream(objectMapper.writeValueAsBytes(requestBody));
        expect(requestContext.getEntityStream()).andReturn(in);

        Capture<InputStream> inputStreamCapture = newCapture();
        requestContext.setEntityStream(capture(inputStreamCapture));
        replay(requestContext, uriInfo);

        ConnectSecretManagementFilter.ConnectRequestContent requestContent = ConnectSecretManagementFilter.ConnectRequestContent.parseFromRequestContext(requestContext);
        assertEquals(connectorName, requestContent.getConnectorName());
        assertEquals(ConnectRequestType.CREATE, requestContent.getRequestType());
        assertEquals(config, requestContent.getConfig());
        assertEquals(requestBody, OBJECT_MAPPER.readValue(inputStreamCapture.getValue(), new TypeReference<Map<String, Object>>() {
        }));
        verify(requestContext, uriInfo);
    }

    @Test
    public void testParseEditRequestContent() throws IOException {
        String connectorName = "testParseEditRequestContent";

        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        expect(requestContext.getMethod()).andReturn(HttpMethod.PUT);
        UriInfo uriInfo = mock(UriInfo.class);
        expect(uriInfo.getPath()).andReturn("connectors/" + connectorName + "/config").anyTimes();
        expect(requestContext.getUriInfo()).andReturn(uriInfo).anyTimes();

        Map<String, String> config = new HashMap<>();
        config.put("prop1", "va1");
        config.put("prop2", "va2");

        InputStream in = new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(config));
        expect(requestContext.getEntityStream()).andReturn(in);

        Capture<InputStream> inputStreamCapture = newCapture();
        requestContext.setEntityStream(capture(inputStreamCapture));
        replay(requestContext, uriInfo);

        ConnectSecretManagementFilter.ConnectRequestContent requestContent = ConnectSecretManagementFilter.ConnectRequestContent.parseFromRequestContext(requestContext);
        assertEquals(connectorName, requestContent.getConnectorName());
        assertEquals(ConnectRequestType.EDIT, requestContent.getRequestType());
        assertEquals(config, requestContent.getConfig());
        assertEquals(config, OBJECT_MAPPER.readValue(inputStreamCapture.getValue(), new TypeReference<Map<String, String>>() {
        }));
        verify(requestContext, uriInfo);
    }

    @Test
    public void testParseDeleteRequestContent() {
        String connectorName = "testParseDeleteRequestContent";

        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        expect(requestContext.getMethod()).andReturn(HttpMethod.DELETE);
        UriInfo uriInfo = mock(UriInfo.class);
        expect(uriInfo.getPath()).andReturn("connectors/" + connectorName).anyTimes();
        expect(requestContext.getUriInfo()).andReturn(uriInfo).anyTimes();

        replay(requestContext, uriInfo);

        ConnectSecretManagementFilter.ConnectRequestContent requestContent = ConnectSecretManagementFilter.ConnectRequestContent.parseFromRequestContext(requestContext);
        assertEquals(connectorName, requestContent.getConnectorName());
        assertEquals(ConnectRequestType.DELETE, requestContent.getRequestType());
        assertNull(requestContent.getConfig());

        verify(requestContext, uriInfo);
    }
}
