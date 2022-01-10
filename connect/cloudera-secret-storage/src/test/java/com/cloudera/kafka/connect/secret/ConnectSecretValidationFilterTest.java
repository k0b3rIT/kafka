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

import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;

import com.cloudera.kafka.connect.common.ConnectRestFilterUtils;

import org.easymock.Capture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.emptyPathMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.emptyPropertyMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.invalidBundleIdMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.invalidPathMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.missingPropertyListedAsSecretMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.pathMismatchMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.propertyMismatchMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.referenceIsInterpolatedMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.referencingNonExistentSecretMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.secretNotInSecretPropertiesListMessage;
import static com.cloudera.kafka.connect.secret.ConnectSecretValidationFilter.secretWithoutBundleIdPresentMessage;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConnectSecretValidationFilterTest {
    private static final String CONFIG_PROVIDER_ALIAS = "secret";
    private static final String CONNECTOR_NAME = "new-connector";
    private static final String BUNDLE_ID = "test_bundle";
    private static final String CONNECTOR_BUNDLE_PATH = CONNECTOR_NAME + "/" + BUNDLE_ID;
    private static final String JDBC_PASSWORD_PROP = "jdbc-password";
    private static final String DELEGATION_TOKEN_PROP = "delegation.token";

    private SecretStorage mockSecretStorage;
    private ConnectSecretValidationFilter secretValidationFilter;

    @BeforeEach
    public void setup() {
        mockSecretStorage = mock(SecretStorage.class);
        secretValidationFilter = new ConnectSecretValidationFilter(mockSecretStorage, CONFIG_PROVIDER_ALIAS);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("validationInputs")
    public void testValidation(String label,
                               Map<String, String> configs,
                               Map<String, String> storedSecrets,
                               Map<ConnectSecretValidationFilter.KeyWithValue, Set<String>> expectedErrors) {
        expect(mockSecretStorage.getSecrets(anyString(), anyString(), eq(false))).andReturn(storedSecrets);
        replay(mockSecretStorage);

        Map<ConnectSecretValidationFilter.KeyWithValue, Set<String>> errors =
                secretValidationFilter.doValidation(CONNECTOR_NAME, configs);

        assertEquals(expectedErrors, errors, label);
    }

    @Test
    public void testCreateResponseFormat() {
        secretValidationFilter = partialMockBuilder(ConnectSecretValidationFilter.class)
                .addMockedMethod("doValidation")
                .withConstructor(mockSecretStorage, CONFIG_PROVIDER_ALIAS)
                .createMock();
        expect(secretValidationFilter.doValidation(anyString(), anyObject())).andReturn(errors());
        ContainerRequestContext mockRequest = createMockRequest("{\"name\":\"my_conn\", \"config\":{}}",
                HttpMethod.POST, "connectors");
        Capture<Response> responseCapture = newCapture();
        mockRequest.abortWith(capture(responseCapture));
        replay(mockRequest, secretValidationFilter);

        secretValidationFilter.filter(mockRequest);

        Response actualResponse = responseCapture.getValue();
        assertErrorMessageResponseContent(actualResponse);
    }

    @Test
    public void testEditResponseFormat() {
        secretValidationFilter = partialMockBuilder(ConnectSecretValidationFilter.class)
                .addMockedMethod("doValidation")
                .withConstructor(mockSecretStorage, CONFIG_PROVIDER_ALIAS)
                .createMock();
        expect(secretValidationFilter.doValidation(anyString(), anyObject())).andReturn(errors());
        ContainerRequestContext mockRequest = createMockRequest("{\"name\":\"my_conn\"}",
                HttpMethod.PUT, "connectors/my_conn/config");
        Capture<Response> responseCapture = newCapture();
        mockRequest.abortWith(capture(responseCapture));
        replay(mockRequest, secretValidationFilter);

        secretValidationFilter.filter(mockRequest);

        Response actualResponse = responseCapture.getValue();
        assertErrorMessageResponseContent(actualResponse);
    }

    @Test
    public void testValidationResponseFormat() {
        secretValidationFilter = partialMockBuilder(ConnectSecretValidationFilter.class)
                .addMockedMethod("doValidation")
                .withConstructor(mockSecretStorage, CONFIG_PROVIDER_ALIAS)
                .createMock();
        expect(secretValidationFilter.doValidation(anyString(), anyObject())).andReturn(errors());
        ContainerRequestContext mockRequest = createMockRequest("{\"name\":\"my_conn\"}",
                HttpMethod.PUT, "connector-plugins/test-type/config/validate");
        Capture<Response> responseCapture = newCapture();
        mockRequest.abortWith(capture(responseCapture));
        replay(mockRequest, secretValidationFilter);

        secretValidationFilter.filter(mockRequest);

        Response actualResponse = responseCapture.getValue();
        assertConfigInfosResponseContent(actualResponse);
    }

    private void assertErrorMessageResponseContent(Response response) {
        ConnectRestFilterUtils.ErrorMessage errorMessage = (ConnectRestFilterUtils.ErrorMessage) response.getEntity();
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), errorMessage.errorCode());
        List<String> expectedMessages = Arrays.asList("error_message_1", "error_message_2", "error_message_3");
        List<String> actualMessages = getErrorMessages(errorMessage);
        assertEquals(expectedMessages, actualMessages);
    }

    private void assertConfigInfosResponseContent(Response response) {
        ConfigInfos configInfos = (ConfigInfos) response.getEntity();
        assertEquals(2, configInfos.values().size());

        ConfigInfo configInfo1 = configInfos.values().get(0);
        assertEquals(JDBC_PASSWORD_PROP, configInfo1.configValue().name());
        assertEquals(Arrays.asList("error_message_1", "error_message_2"), configInfo1.configValue().errors());

        ConfigInfo configInfo2 = configInfos.values().get(1);
        assertEquals(DELEGATION_TOKEN_PROP, configInfo2.configValue().name());
        assertEquals(Collections.singletonList("error_message_3"), configInfo2.configValue().errors());
    }

    private List<String> getErrorMessages(ConnectRestFilterUtils.ErrorMessage msg) {
        String prefixStripped = msg.message().substring(1);
        String suffixStripped = prefixStripped.substring(0, prefixStripped.length() - 1);
        if (suffixStripped.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.asList(suffixStripped.split(", "));
    }

    private Map<ConnectSecretValidationFilter.KeyWithValue, Set<String>> errors() {
        Map<ConnectSecretValidationFilter.KeyWithValue, Set<String>> errors = new LinkedHashMap<>();
        errors.put(
                new ConnectSecretValidationFilter.KeyWithValue(JDBC_PASSWORD_PROP, "some_value"),
                new LinkedHashSet<>(Arrays.asList("error_message_1", "error_message_2"))
        );
        errors.put(
                new ConnectSecretValidationFilter.KeyWithValue(DELEGATION_TOKEN_PROP, "some_other_value"),
                new LinkedHashSet<>(Collections.singletonList("error_message_3"))
        );
        return errors;
    }

    private ContainerRequestContext createMockRequest(String connectorConfig, String methodType, String path) {
        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        expect(requestContext.getMethod()).andReturn(methodType);
        UriInfo uriInfo = createMock(UriInfo.class);
        expect(requestContext.getUriInfo()).andReturn(uriInfo).anyTimes();
        expect(uriInfo.getPath()).andReturn(path).anyTimes();
        ByteArrayInputStream is = new ByteArrayInputStream(connectorConfig.getBytes());
        expect(requestContext.getEntityStream()).andReturn(is);
        replay(uriInfo);
        requestContext.setEntityStream(anyObject());
        return requestContext;
    }

    private static Stream<Arguments> validationInputs() {
        return Stream
                .of(
                        inputEmptyPath(),
                        inputPathMismatch(),
                        inputEmptyProperty(),
                        inputPropertyMismatch(),
                        inputReferenceWithoutBundlePresent(),
                        inputReferenceWithoutPropertyInSecretsPresent(),
                        inputSecretNotInSecretPropertiesList(),
                        inputSecretIsInterpolated(),
                        inputListedSecretPropertyIsNotPresent(),
                        inputSecretBundleIdNotPresent(),
                        inputMultipleErrors(),
                        inputValid()
                )
                .map(ValidationArgumentBuilder::toArgs);
    }

    private static ValidationArgumentBuilder inputEmptyPath() {
        return new ValidationArgumentBuilder("empty path in reference")
                .addBundleId()
                .addSecretPropertiesList(JDBC_PASSWORD_PROP)
                .addConfig(JDBC_PASSWORD_PROP, "${secret::jdbc-password}")
                .addConfig("file-prop", "\"${file:/tmp/path:property-name}")
                .addSecret(JDBC_PASSWORD_PROP, "secret_pw")
                .addErrors(JDBC_PASSWORD_PROP,
                        emptyPathMessage("secret::" + JDBC_PASSWORD_PROP),
                        pathMismatchMessage("secret::" + JDBC_PASSWORD_PROP, "new-connector/" + BUNDLE_ID),
                        invalidPathMessage("secret::" + JDBC_PASSWORD_PROP));
    }

    private static ValidationArgumentBuilder inputPathMismatch() {
        return new ValidationArgumentBuilder("connector name and reference path does not match")
                .addBundleId()
                .addSecretPropertiesList(JDBC_PASSWORD_PROP)
                .addConfig(JDBC_PASSWORD_PROP, "${secret:not-the-actual-connector/" + BUNDLE_ID + ":" + JDBC_PASSWORD_PROP + "}")
                .addSecret(JDBC_PASSWORD_PROP, "secret_pw")
                .addErrors(JDBC_PASSWORD_PROP,
                        pathMismatchMessage("secret:not-the-actual-connector/" + BUNDLE_ID + ":" + JDBC_PASSWORD_PROP, CONNECTOR_BUNDLE_PATH));
    }

    private static ValidationArgumentBuilder inputEmptyProperty() {
        return new ValidationArgumentBuilder("empty property in reference")
                .addBundleId()
                .addSecretPropertiesList(JDBC_PASSWORD_PROP)
                .addConfig(JDBC_PASSWORD_PROP, "${secret:" + CONNECTOR_BUNDLE_PATH + ":}")
                .addConfig("file-prop", "\"${file:/tmp/path:property-name}")
                .addSecret(JDBC_PASSWORD_PROP, "secret_pw")
                .addErrors(JDBC_PASSWORD_PROP,
                        emptyPropertyMessage("secret:" + CONNECTOR_BUNDLE_PATH + ":"),
                        propertyMismatchMessage("secret:" + CONNECTOR_BUNDLE_PATH + ":", JDBC_PASSWORD_PROP));
    }

    private static ValidationArgumentBuilder inputPropertyMismatch() {
        return new ValidationArgumentBuilder("property key and reference property does not match")
                .addBundleId()
                .addSecretPropertiesList(JDBC_PASSWORD_PROP)
                .addConfig(JDBC_PASSWORD_PROP, secretRef("not-matching-property"))
                .addSecret(JDBC_PASSWORD_PROP, "secret_pw")
                .addErrors(JDBC_PASSWORD_PROP, propertyMismatchMessage(secretRefInner("not-matching-property"), JDBC_PASSWORD_PROP));
    }

    private static ValidationArgumentBuilder inputReferenceWithoutBundlePresent() {
        return new ValidationArgumentBuilder("reference without bundle id present")
                .addSecretPropertiesList(JDBC_PASSWORD_PROP)
                .addConfig(JDBC_PASSWORD_PROP, secretRef(JDBC_PASSWORD_PROP))
                .addErrors(JDBC_PASSWORD_PROP, secretWithoutBundleIdPresentMessage(secretRefInner(JDBC_PASSWORD_PROP)));
    }

    private static ValidationArgumentBuilder inputReferenceWithoutPropertyInSecretsPresent() {
        return new ValidationArgumentBuilder("reference without secret property present in storage")
                .addBundleId()
                .addSecretPropertiesList(JDBC_PASSWORD_PROP)
                .addSecret("different_secret", "some_other_secret_value")
                .addConfig(JDBC_PASSWORD_PROP, secretRef(JDBC_PASSWORD_PROP))
                .addErrors(JDBC_PASSWORD_PROP, referencingNonExistentSecretMessage(JDBC_PASSWORD_PROP));
    }

    private static ValidationArgumentBuilder inputSecretNotInSecretPropertiesList() {
        return new ValidationArgumentBuilder("referenced property is not in " + SecretStorageExtension.SENSITIVE_PROPERTY_LIST)
                .addBundleId()
                .addConfig(JDBC_PASSWORD_PROP, secretRef(JDBC_PASSWORD_PROP))
                .addSecret(JDBC_PASSWORD_PROP, "secret_pw")
                .addErrors(JDBC_PASSWORD_PROP,
                        secretNotInSecretPropertiesListMessage(secretRefInner(JDBC_PASSWORD_PROP), JDBC_PASSWORD_PROP));
    }

    private static ValidationArgumentBuilder inputSecretIsInterpolated() {
        return new ValidationArgumentBuilder("secret is interpolated into the value")
                .addBundleId()
                .addSecretPropertiesList(JDBC_PASSWORD_PROP)
                .addConfig(JDBC_PASSWORD_PROP, "bla " + secretRef(JDBC_PASSWORD_PROP) + " bla")
                .addSecret(JDBC_PASSWORD_PROP, "secret_pw")
                .addErrors(JDBC_PASSWORD_PROP, referenceIsInterpolatedMessage(JDBC_PASSWORD_PROP));
    }

    private static ValidationArgumentBuilder inputListedSecretPropertyIsNotPresent() {
        return new ValidationArgumentBuilder("secret is listed in " + SecretStorageExtension.SENSITIVE_PROPERTY_LIST + " but missing")
                .addBundleId()
                .addSecretPropertiesList(JDBC_PASSWORD_PROP)
                .addSecret(JDBC_PASSWORD_PROP, "secret_pw")
                .addErrorsWithValue(JDBC_PASSWORD_PROP, null, missingPropertyListedAsSecretMessage(JDBC_PASSWORD_PROP));
    }

    private static ValidationArgumentBuilder inputSecretBundleIdNotPresent() {
        return new ValidationArgumentBuilder("secret bundle id is not present in storage")
                .addBundleId()
                .addErrors(SecretStorageExtension.SECRET_BUNDLE_ID, invalidBundleIdMessage(BUNDLE_ID));
    }

    private static ValidationArgumentBuilder inputMultipleErrors() {
        return new ValidationArgumentBuilder("multiple invalid properties")
                .addBundleId()
                .addSecretPropertiesList(JDBC_PASSWORD_PROP, DELEGATION_TOKEN_PROP)
                .addConfig(JDBC_PASSWORD_PROP, "${secret:not-actual-connector:" + JDBC_PASSWORD_PROP + "}")
                .addConfig(DELEGATION_TOKEN_PROP, secretRef("different-property"))
                .addSecret(JDBC_PASSWORD_PROP, "secret_pw")
                .addSecret(DELEGATION_TOKEN_PROP, "secret_token")
                .addErrors(JDBC_PASSWORD_PROP,
                        invalidPathMessage("secret:not-actual-connector:" + JDBC_PASSWORD_PROP),
                        pathMismatchMessage("secret:not-actual-connector:" + JDBC_PASSWORD_PROP, CONNECTOR_BUNDLE_PATH))
                .addErrors(DELEGATION_TOKEN_PROP,
                        propertyMismatchMessage(secretRefInner("different-property"), DELEGATION_TOKEN_PROP));
    }

    private static ValidationArgumentBuilder inputValid() {
        return new ValidationArgumentBuilder("valid input")
                .addBundleId()
                .addSecretPropertiesList(JDBC_PASSWORD_PROP, DELEGATION_TOKEN_PROP)
                .addConfig(JDBC_PASSWORD_PROP, secretRef(JDBC_PASSWORD_PROP))
                .addConfig(DELEGATION_TOKEN_PROP, "secret_token_new_value")
                .addSecret(JDBC_PASSWORD_PROP, "secret_pw");
    }

    private static String secretRef(String property) {
        return "${" + secretRefInner(property) + "}";
    }

    private static String secretRefInner(String property) {
        return CONFIG_PROVIDER_ALIAS + ":" + CONNECTOR_BUNDLE_PATH + ":" + property;
    }

    private static class ValidationArgumentBuilder {
        private final String label;
        private Map<String, String> configs;
        private Map<String, String> secrets;
        private final Map<ConnectSecretValidationFilter.KeyWithValue, Set<String>> errors = new HashMap<>();

        public ValidationArgumentBuilder(String label) {
            this.label = label;
        }

        ValidationArgumentBuilder addSecretPropertiesList(String... properties) {
            return addConfig(SecretStorageExtension.SENSITIVE_PROPERTY_LIST,
                    String.join(",", properties));
        }

        ValidationArgumentBuilder addBundleId() {
            return addConfig(SecretStorageExtension.SECRET_BUNDLE_ID, BUNDLE_ID);
        }

        ValidationArgumentBuilder addConfig(String key, String value) {
            if (configs == null) {
                configs = new HashMap<>();
            }
            configs.put(key, value);
            return this;
        }

        ValidationArgumentBuilder addSecret(String key, String value) {
            if (secrets == null) {
                secrets = new HashMap<>();
            }
            secrets.put(key, value);
            return this;
        }

        ValidationArgumentBuilder addErrors(String key, String... propErrors) {
            return addErrorsWithValue(key, configs.get(key), propErrors);
        }

        ValidationArgumentBuilder addErrorsWithValue(String key, String value, String... propErrors) {
            errors.put(
                    new ConnectSecretValidationFilter.KeyWithValue(key, value),
                    new HashSet<>(Arrays.asList(propErrors))
            );
            return this;
        }

        Arguments toArgs() {
            return Arguments.of(label, configs, secrets, errors);
        }
    }
}
