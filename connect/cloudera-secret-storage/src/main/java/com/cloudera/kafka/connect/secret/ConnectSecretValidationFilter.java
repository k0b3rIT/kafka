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

import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;

import com.cloudera.kafka.connect.common.ConnectRestFilterUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Priority;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;

import static com.cloudera.kafka.connect.secret.SecretStorageExtension.SECRET_BUNDLE_ID;

@Priority(Priorities.USER + 1)
public class ConnectSecretValidationFilter implements ContainerRequestFilter {

    private static final String PATH_NOT_MATCHING_CONNECTOR_NAME_MESSAGE_TEMPLATE =
            "Invalid secret reference. The path in secret reference {%s} does not match the expected path of {%s}.";
    private static final String PATH_INVALID_FORMAT_MESSAGE_TEMPLATE =
            "Invalid secret reference. The path in secret reference {%s} is not in a valid format.";
    private static final String EMPTY_PATH_MESSAGE_TEMPLATE =
            "Invalid secret reference. The path in secret {%s} must not be empty.";
    private static final String EMPTY_PROPERTY_KEY_MESSAGE_TEMPLATE =
            "Invalid secret reference. The property key in secret {%s} must not be empty.";
    private static final String REFERENCE_VARIABLE_NOT_MATCHING_PROPERTY_KEY_MESSAGE_TEMPLATE =
            "Invalid secret reference. The property key part of the reference {%s} does not match the property key {%s}.";
    private static final String VARIABLE_NOT_FULL_REFERENCE_MESSAGE_TEMPLATE =
            "Invalid secret reference. The value in property {%s} either needs to be a new value, or a secret reference.";
    private static final String SENSITIVE_PROPERTIES_DOES_NOT_CONTAIN_SECRET_REFERENCE_KEY_MESSAGE_TEMPLATE =
            "Invalid secret reference. The secret reference {%s} refers to property %s which is not present in "
                    + SecretStorageExtension.SENSITIVE_PROPERTY_LIST + ".";
    private static final String SECRET_REFERENCE_WITHOUT_SECRET_BUNDLE_ID_PRESENT_MESSAGE_TEMPLATE =
            "Invalid secret reference. The secret reference {%s} is present without a proper bundle id defined under " + SECRET_BUNDLE_ID + ".";
    private static final String CONFIG_DOES_NOT_CONTAIN_SENSITIVE_PROPERTY_MESSAGE_TEMPLATE =
            "Invalid secret configuration. The configuration does not contain the property {%s} listed in "
                    + SecretStorageExtension.SENSITIVE_PROPERTY_LIST + ".";
    private static final String SECRET_BUNDLE_ID_INVALID_MESSAGE_TEMPLATE =
            "Invalid secret configuration. The secret.bundle.id {%s} is invalid. Either it was deleted or it is in the wrong format.";
    private static final String SECRET_REFERENCE_NOT_IN_EXISTING_SECRETS_MESSAGE_TEMPLATE =
            "Invalid secret configuration. The secret reference {%s} cannot be edited, it is not part of the existing secrets.";

    private static final Logger log = LoggerFactory.getLogger(ConnectSecretValidationFilter.class);

    private static final Pattern CONNECTOR_PLUGIN_VALIDATE_REQUEST_PATTERN = Pattern.compile("^connector-plugins/[^/]+/config/validate[/]?");
    private static final Pattern CONNECTOR_LIST_OR_CREATE_PATTERN = Pattern.compile("^connectors[/]?");
    private static final Pattern CONNECTOR_CONFIG_REQUEST_PATTERN = Pattern.compile("^connectors/([^/]+)/config[/]?");


    private static final List<ConnectRestFilterUtils.MethodType> ALLOWED_METHOD_LIST = Arrays.asList(
            new ConnectRestFilterUtils.MethodType(HttpMethod.POST, CONNECTOR_LIST_OR_CREATE_PATTERN),
            new ConnectRestFilterUtils.MethodType(HttpMethod.PUT, CONNECTOR_CONFIG_REQUEST_PATTERN),
            new ConnectRestFilterUtils.MethodType(HttpMethod.PUT, CONNECTOR_PLUGIN_VALIDATE_REQUEST_PATTERN)
    );

    private final SecretStorage secretStorage;
    private final String secretProviderAlias;

    public ConnectSecretValidationFilter(SecretStorage secretStorage, String secretProviderAlias) {
        this.secretStorage = secretStorage;
        this.secretProviderAlias = secretProviderAlias;
    }

    static String invalidPathMessage(Object reference) {
        return String.format(PATH_INVALID_FORMAT_MESSAGE_TEMPLATE, reference);
    }

    static String pathMismatchMessage(Object reference, String expectedPath) {
        return String.format(PATH_NOT_MATCHING_CONNECTOR_NAME_MESSAGE_TEMPLATE, reference, expectedPath);
    }

    static String propertyMismatchMessage(Object reference, String property) {
        return String.format(REFERENCE_VARIABLE_NOT_MATCHING_PROPERTY_KEY_MESSAGE_TEMPLATE, reference, property);
    }

    static String emptyPathMessage(Object reference) {
        return String.format(EMPTY_PATH_MESSAGE_TEMPLATE, reference);
    }

    static String emptyPropertyMessage(Object reference) {
        return String.format(EMPTY_PROPERTY_KEY_MESSAGE_TEMPLATE, reference);
    }

    static String referenceIsInterpolatedMessage(String property) {
        return String.format(VARIABLE_NOT_FULL_REFERENCE_MESSAGE_TEMPLATE, property);
    }

    static String secretWithoutBundleIdPresentMessage(Object reference) {
        return String.format(SECRET_REFERENCE_WITHOUT_SECRET_BUNDLE_ID_PRESENT_MESSAGE_TEMPLATE, reference);
    }

    static String secretNotInSecretPropertiesListMessage(Object reference, String property) {
        return String.format(SENSITIVE_PROPERTIES_DOES_NOT_CONTAIN_SECRET_REFERENCE_KEY_MESSAGE_TEMPLATE, reference, property);
    }

    static String missingPropertyListedAsSecretMessage(String property) {
        return String.format(CONFIG_DOES_NOT_CONTAIN_SENSITIVE_PROPERTY_MESSAGE_TEMPLATE, property);
    }

    static String invalidBundleIdMessage(String bundleId) {
        return String.format(SECRET_BUNDLE_ID_INVALID_MESSAGE_TEMPLATE, bundleId);
    }

    static String referencingNonExistentSecretMessage(String property) {
        return String.format(SECRET_REFERENCE_NOT_IN_EXISTING_SECRETS_MESSAGE_TEMPLATE, property);
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        String httpMethod = requestContext.getMethod();
        String path = requestContext.getUriInfo().getPath();
        Optional<ConnectRestFilterUtils.MethodType> methodType = ALLOWED_METHOD_LIST.stream()
                .filter(method -> method.matchingWithMethodAndPath(httpMethod, path))
                .findFirst();
        if (!methodType.isPresent()) {
            return;
        }
        ConnectContent content = getConnectContentBasedOnMethodType(methodType.get(), requestContext);
        Map<KeyWithValue, Set<String>> validationErrors = doValidation(content.getName(), content.getConfig());
        if (!validationErrors.isEmpty()) {
            abortRequestWithCorrectMessageFormat(requestContext, methodType.get(), validationErrors);
        }
    }

    // Visible for testing
    Map<KeyWithValue, Set<String>> doValidation(String connectorName, Map<String, String> configMap) {
        ValidationErrors errors = new ValidationErrors();

        //Check secret.bundle.id
        String secretBundleId = configMap.remove(SECRET_BUNDLE_ID);

        Set<String> existingSecrets = null;
        if (secretBundleId != null) {
            // If we have secret bundle id, we need to validate it using the secret store.
            Map<String, String> existingSecretProperties = secretStorage.getSecrets(connectorName, secretBundleId, false);
            if (existingSecretProperties == null) {
                errors.add(SECRET_BUNDLE_ID, secretBundleId, invalidBundleIdMessage(secretBundleId));
            } else {
                existingSecrets = existingSecretProperties.keySet();
            }
        }

        Map<KeyWithValue, ConfigVariable> secretVars = getSecretVarsFromConfigMap(configMap);
        String sensitiveProperties = configMap.getOrDefault(SecretStorageExtension.SENSITIVE_PROPERTY_LIST, "");

        Set<String> sensitivePropertiesSet = sensitiveProperties.isEmpty() ?
                Collections.emptySet() : new HashSet<>(Arrays.asList(sensitiveProperties.split(",")));

        ValidationErrors propertyErrors = processSecretVars(secretVars, sensitivePropertiesSet,
                connectorName, existingSecrets, secretBundleId);
        errors.addAll(propertyErrors);

        //All the properties listed in secret.properties should be present in the config
        ValidationErrors missingSensitivePropertiesErrorMessages =
                checkMissingSensitiveProperties(sensitivePropertiesSet, configMap);
        errors.addAll(missingSensitivePropertiesErrorMessages);

        if (!errors.propertyErrorMessageMap.isEmpty()) {
            log.trace("Validation errors found for connector {}: {}", connectorName, errors.propertyErrorMessageMap);
        }
        return errors.propertyErrorMessageMap;
    }

    private ValidationErrors checkMissingSensitiveProperties(Set<String> sensitivePropertiesSet, Map<String, String> configMap) {
        ValidationErrors errors = new ValidationErrors();
        Set<String> missingSensitiveProperties = sensitivePropertiesSet.stream()
                .filter(sp -> !configMap.containsKey(sp))
                .collect(Collectors.toSet());
        if (!missingSensitiveProperties.isEmpty()) {
            for (String msp : missingSensitiveProperties) {
                errors.add(msp, missingPropertyListedAsSecretMessage(msp));
            }
        }
        return errors;
    }

    private ValidationErrors processSecretVars(Map<KeyWithValue, ConfigVariable> secretVars,
                                               Set<String> sensitivePropertiesSet, String connectorName,
                                               Set<String> existingSecrets, String bundleId) {
        ValidationErrors errors = new ValidationErrors();
        secretVars.forEach((key, value) -> {
            List<String> messages = doFiltering(key, value, sensitivePropertiesSet, connectorName, existingSecrets, bundleId);
            if (!messages.isEmpty()) {
                errors.add(key, messages);
            }
        });
        return errors;
    }

    private List<String> doFiltering(KeyWithValue kv, ConfigVariable secretVar, Set<String> sensitivePropertiesSet,
                                     String connectorName, Set<String> existingSecrets, String bundleId) {
        List<String> errorMessages = new ArrayList<>();

        if (secretVar.path.isEmpty()) {
            errorMessages.add(emptyPathMessage(secretVar));
        }
        if (secretVar.variable.isEmpty()) {
            errorMessages.add(emptyPropertyMessage(secretVar));
        }

        //The property key inside the reference must be equal to the property key
        if (!secretVar.variable.equals(kv.propertyKey)) {
            errorMessages.add(propertyMismatchMessage(secretVar, kv.propertyKey));
        }

        if (bundleId != null) {
            errorMessages.addAll(checkBundleId(secretVar, connectorName, bundleId));
        }

        //The property value must be the whole secret reference e.g: "bla ${secret:conn:mypass} bla" is not allowed
        if (secretVar.matchStartIdx != 0 || secretVar.matchEndIdx != secretVar.getFullPropertyValue().length()) {
            errorMessages.add(referenceIsInterpolatedMessage(kv.propertyKey));
        }

        //Secret references must be in secret.properties
        if (!sensitivePropertiesSet.contains(kv.propertyKey)) {
            String message = secretNotInSecretPropertiesListMessage(secretVar, kv.propertyKey);
            errorMessages.add(message);
        }

        //If we have existing secrets, there must be existing secrets, and the property should be among them
        if (existingSecrets == null) {
            errorMessages.add(secretWithoutBundleIdPresentMessage(secretVar));
        } else if (!existingSecrets.contains(kv.propertyKey)) {
            errorMessages.add(referencingNonExistentSecretMessage(kv.propertyKey));
        }

        return errorMessages;
    }

    private List<String> checkBundleId(ConfigVariable secretVar, String connectorName, String bundleId) {
        List<String> errorMessages = new ArrayList<>();
        BundlePath expectedPath = new BundlePath(connectorName, bundleId);
        BundlePath bundlePath = BundlePath.parseFromPath(secretVar.path);
        if (bundlePath == null) {
            errorMessages.add(invalidPathMessage(secretVar));
        }
        if (!expectedPath.equals(bundlePath)) {
            errorMessages.add(pathMismatchMessage(secretVar, expectedPath.toPath()));
        }
        return errorMessages;
    }

    private void abortRequestWithCorrectMessageFormat(ContainerRequestContext requestContext,
                                                      ConnectRestFilterUtils.MethodType methodType,
                                                      Map<KeyWithValue, Set<String>> propertyErrorMessageMap) {
        if (methodType.getPathPattern().equals(CONNECTOR_PLUGIN_VALIDATE_REQUEST_PATTERN)) {
            List<ConfigInfo> configInfoList = propertyErrorMessageMap.entrySet().stream().map(e -> {
                ConfigValueInfo valueInfo = new ConfigValueInfo(
                        e.getKey().propertyKey,
                        e.getKey().originalValue,
                        Collections.emptyList(),
                        new ArrayList<>(e.getValue()),
                        true
                );
                return new ConfigInfo(null, valueInfo);
            }).sorted(Comparator.comparing(ci -> ci.configValue().errors().toString())).collect(Collectors.toList());
            ConfigInfos configInfos = new ConfigInfos("", propertyErrorMessageMap.size(), Collections.emptyList(),
                    configInfoList);
            requestContext.abortWith(
                    Response.status(Response.Status.OK).entity(configInfos).build()
            );
        } else {
            Response.Status status = Response.Status.BAD_REQUEST;
            List<String> errors = propertyErrorMessageMap.values().stream().flatMap(Collection::stream).sorted().collect(Collectors.toList());
            requestContext.abortWith(
                    Response.status(status)
                            .entity(new ConnectRestFilterUtils.ErrorMessage(status.getStatusCode(), errors.toString())).build()
            );
        }
    }

    private Map<KeyWithValue, ConfigVariable> getSecretVarsFromConfigMap(Map<String, String> configMap) {
        Map<KeyWithValue, ConfigVariable> configVars = new HashMap<>();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            Matcher matcher = ConfigTransformer.DEFAULT_PATTERN.matcher(entry.getValue());
            while (matcher.find()) {
                if (secretProviderAlias.equals(matcher.group(1))) {
                    configVars.put(new KeyWithValue(entry.getKey(), entry.getValue()), new ConfigVariable(matcher));
                }
            }
        }
        return configVars;
    }

    private ConnectContent getConnectContentFromRequest(ContainerRequestContext requestContext) {
        try {
            byte[] content;
            try (InputStream inputStream = requestContext.getEntityStream()) {
                content = ConnectRestFilterUtils.inputStreamToByteArray(inputStream);
            }
            if (content == null) {
                throw new ConnectRestException(Response.Status.BAD_REQUEST, "Invalid request body, could not parse configuration");
            }
            requestContext.setEntityStream(new ByteArrayInputStream(content));
            ObjectMapper mapper = new ObjectMapper();
            ConnectContent connectContent = mapper.readValue(content, ConnectContent.class);
            if (connectContent.config.containsKey("name")) {
                if (!connectContent.config.get("name").equals(connectContent.name)) {
                    String msg = "Property values for 'name' and 'config.name' do not match.";
                    log.error("Connector connect reading failed with invalid body. " + msg);
                    throw new ConnectRestException(Response.Status.BAD_REQUEST, msg);
                }
            }
            return connectContent;
        } catch (IOException e) {
            log.error("Connector connect reading failed with invalid body.", e);
            throw new ConnectRestException(Response.Status.BAD_REQUEST, "Invalid request body, could not parse configuration", e);
        }
    }

    private ConnectContent getConnectContentBasedOnMethodType(ConnectRestFilterUtils.MethodType methodType, ContainerRequestContext requestContext) {
        ConnectContent content;
        if (methodType.getPathPattern().equals(CONNECTOR_LIST_OR_CREATE_PATTERN)) {
            content = getConnectContentFromRequest(requestContext);
        } else {
            Map<String, String> configMap = getConfigMapFromRequest(requestContext);
            String connectorName = configMap.get(ConnectorConfig.NAME_CONFIG);
            if (methodType.getPathPattern().equals(CONNECTOR_CONFIG_REQUEST_PATTERN)) {
                Matcher matcher = methodType.getPathPattern().matcher(requestContext.getUriInfo().getPath());
                if (!matcher.matches()) {
                    log.error("Encountered path matching issue - this should never happen");
                    throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR, "Unexpected error");
                }
                String connectorNameInPath = matcher.group(1);
                if (connectorName != null && !connectorNameInPath.equals(connectorName)) {
                    throw new ConnectRestException(Response.Status.BAD_REQUEST,
                            "Connector name in path must match the name in the content");
                }
                connectorName = connectorNameInPath;
            }
            content = new ConnectContent(configMap, connectorName);
        }
        return content;
    }

    private Map<String, String> getConfigMapFromRequest(ContainerRequestContext requestContext) {
        try {
            byte[] content;
            try (InputStream inputStream = requestContext.getEntityStream()) {
                content = ConnectRestFilterUtils.inputStreamToByteArray(inputStream);
            }
            if (content == null) {
                throw new ConnectRestException(Response.Status.BAD_REQUEST, "Invalid request body, could not parse configuration");
            }
            requestContext.setEntityStream(new ByteArrayInputStream(content));
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(content, new TypeReference<Map<String, String>>() {
            });
        } catch (IOException e) {
            log.error("Connector connect reading failed with invalid body.", e);
            throw new ConnectRestException(Response.Status.BAD_REQUEST, "Invalid request body, could not parse configuration", e);
        }
    }

    static final class KeyWithValue {
        final String propertyKey;
        final String originalValue;

        KeyWithValue(String propertyKey, String originalValue) {
            this.propertyKey = propertyKey;
            this.originalValue = originalValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeyWithValue that = (KeyWithValue) o;
            return propertyKey.equals(that.propertyKey) && Objects.equals(originalValue, that.originalValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyKey, originalValue);
        }

        @Override
        public String toString() {
            return "KeyWithValue{" +
                    "propertyKey='" + propertyKey + '\'' +
                    ", originalValue='" + originalValue + '\'' +
                    '}';
        }
    }

    private static class ValidationErrors {
        final Map<KeyWithValue, Set<String>> propertyErrorMessageMap = new HashMap<>();

        void add(String key, String originalValue, String error) {
            errorsFor(new KeyWithValue(key, originalValue)).add(error);
        }

        void add(String key, String error) {
            errorsFor(new KeyWithValue(key, null)).add(error);
        }

        void add(KeyWithValue key, Collection<String> errors) {
            errorsFor(key).addAll(errors);
        }

        void addAll(ValidationErrors messages) {
            messages.propertyErrorMessageMap.forEach((k, errors) -> errorsFor(k).addAll(errors));
        }

        private Set<String> errorsFor(KeyWithValue key) {
            return propertyErrorMessageMap.computeIfAbsent(key, k -> new LinkedHashSet<>());
        }
    }

    @JsonIgnoreProperties(value = { "initial_state" }) //initial_state can be determined only from the connector create request
    private static class ConnectContent {
        private final String name;
        private final Map<String, String> config;

        @JsonCreator
        public ConnectContent(@JsonProperty("config") Map<String, String> config,
                              @JsonProperty("name") String name) {
            this.config = config;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public Map<String, String> getConfig() {
            return config;
        }
    }

    static class ConfigVariable {
        final String providerName;
        final String path;
        final String variable;
        final int matchStartIdx;
        final int matchEndIdx;

        ConfigVariable(Matcher matcher) {
            this.providerName = matcher.group(1);
            this.path = matcher.group(3) != null ? matcher.group(3) : "";
            this.variable = matcher.group(4);
            this.matchStartIdx = matcher.regionStart();
            this.matchEndIdx = matcher.regionEnd();
        }

        public String getFullPropertyValue() {
            return String.format("${%s:%s:%s}", providerName, path, variable);
        }

        public String toString() {
            return providerName + ":" + (path != null ? path + ":" : "") + variable;
        }
    }

}
