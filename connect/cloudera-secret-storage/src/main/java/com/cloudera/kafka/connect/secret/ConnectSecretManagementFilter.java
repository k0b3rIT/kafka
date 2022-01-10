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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response.Status;

import static com.cloudera.kafka.connect.common.ConnectRestFilterUtils.extractConnectorNameFromPath;
import static com.cloudera.kafka.connect.common.ConnectRestFilterUtils.inputStreamToByteArray;
import static com.cloudera.kafka.connect.secret.ConnectSecretManagementFilter.SecretStorageReference.isSecretStorageReference;
import static com.cloudera.kafka.connect.secret.SecretStorageExtension.SECRET_BUNDLE_ID;

@Priority(Priorities.USER + 2)
public class ConnectSecretManagementFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectSecretManagementFilter.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String REQUEST_PROPERTY_PREFIX = ConnectSecretManagementFilter.class.getName();
    private static final String REQUEST_PROPERTY_NEW_UUID = REQUEST_PROPERTY_PREFIX + "new.bundle.uuid";
    private static final String REQUEST_PROPERTY_BASE_UUID = REQUEST_PROPERTY_PREFIX + "base.bundle.uuid";
    private static final String REQUEST_PROPERTY_CONNECTOR_NAME = REQUEST_PROPERTY_PREFIX + "connector.name";
    private static final String REQUEST_PROPERTY_DELETE_ALL_SECRETS = REQUEST_PROPERTY_PREFIX + "delete.all.secrets";

    private static final List<ConnectRequestType> ALLOWED_REQUEST_TYPES =
            Arrays.asList(ConnectRequestType.CREATE, ConnectRequestType.EDIT, ConnectRequestType.DELETE);

    private final SecretStorage secretStorage;
    private final ConnectClusterState clusterState;
    private final String secretProviderAlias;

    public ConnectSecretManagementFilter(SecretStorage secretStorage, ConnectClusterState clusterState,
                                         String secretProviderAlias) {
        this.secretStorage = secretStorage;
        this.clusterState = clusterState;
        this.secretProviderAlias = secretProviderAlias;
    }

    /**
     * package-private for testing
     *
     * @return Key-Value map of configs that are marked as sensitive, based on the value of {@link
     * SecretStorageExtension#SENSITIVE_PROPERTY_LIST} property in the config map.
     * Empty map if the value of {@link SecretStorageExtension#SENSITIVE_PROPERTY_LIST} is either empty or null
     */
    static Map<String, String> getSecretConfigs(Map<String, String> configMap) {
        String secretPropertiesList = configMap.get(SecretStorageExtension.SENSITIVE_PROPERTY_LIST);
        if (secretPropertiesList == null || secretPropertiesList.isEmpty()) {
            return Collections.emptyMap();
        }
        Set<String> secretProperties = Arrays.stream(secretPropertiesList.split(","))
                .collect(Collectors.toSet());
        Map<String, String> secrets = new HashMap<>(configMap);
        secrets.keySet().retainAll(secretProperties);
        return secrets;
    }

    /**
     * package-private for testing
     * <p>
     * Replace values of secret properties listed in {@link
     * SecretStorageExtension#SENSITIVE_PROPERTY_LIST} with secret references. Does not change the map
     * passed to this method, instead creates a copy and replaces the values in the copy.
     *
     * @return Key-Value config map with redacted secret config values
     */
    static Map<String, String> redactSecretConfigs(String providerAlias, String connectorName, String id,
                                                   Map<String, String> configs) {
        Set<String> secretProperties = getSecretConfigs(configs).keySet();
        if (id == null || secretProperties.isEmpty()) {
            return new HashMap<>(configs);
        }
        BundlePath path = new BundlePath(connectorName, id);
        Map<String, String> redactedConfigs = new HashMap<>(configs);
        secretProperties.forEach(x ->
                redactedConfigs.computeIfPresent(x,
                        (key, value) -> new SecretStorageReference(providerAlias, path, key).toString()));
        return redactedConfigs;
    }

    private static void setRequestBody(ContainerRequestContext requestContext, Map<String, ?> requestBody)
            throws ConnectRestException {
        try {
            requestContext.setEntityStream(
                    new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(requestBody)));
        } catch (JsonProcessingException e) {
            String msg = "Error parsing filtered JSON back into request body. " + e.getMessage();
            LOGGER.error(msg);
            throw new ConnectRestException(Status.INTERNAL_SERVER_ERROR, msg, e);
        }
    }

    /**
     * <pre>
     * request filter - handles following endpoints:
     * - POST /connectors/ ({@link org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource#createConnector}):
     * - PUT /connectors/{connector}/config/ ({@link org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource#putConnectorConfig}):
     *
     * In both cases, saves secrets defined under {@link SecretStorageExtension#SENSITIVE_PROPERTY_LIST} to the secret storage,
     * and replaces all plaintext secret values with secret references.
     * </pre>
     *
     * @param requestContext requestContext
     */
    @Override
    public void filter(ContainerRequestContext requestContext) {
        ConnectRequestType requestType = ConnectRequestContent.getRequestType(requestContext);

        if (!ALLOWED_REQUEST_TYPES.contains(requestType)) {
            return;
        }

        ConnectRequestContent requestContent = ConnectRequestContent.parseFromRequestContext(requestContext);
        String connectorName = requestContent.connectorName;
        requestContext.setProperty(REQUEST_PROPERTY_CONNECTOR_NAME, connectorName);

        if (requestType.equals(ConnectRequestType.DELETE)) {
            // try to extract the base bundle id from the connector configs,
            // pass to the response filter for cleanup
            try {
                Map<String, String> connectorConfig = clusterState.connectorConfig(connectorName);
                String baseUuid = connectorConfig.get(SECRET_BUNDLE_ID);
                requestContext.setProperty(REQUEST_PROPERTY_BASE_UUID, baseUuid);
            } catch (ConnectException e) {
                //Thrown when the connector does not exist - can be ignored
            }
            return;
        }

        Map<String, String> incomingConfigs = requestContent.config;
        Map<String, String> incomingSecrets = getSecretConfigs(incomingConfigs);
        String baseUuid = incomingConfigs.get(SECRET_BUNDLE_ID);
        requestContext.setProperty(REQUEST_PROPERTY_BASE_UUID, baseUuid);
        // retrieve all secrets for base uuid
        Map<String, String> upstreamSecrets = getUpstreamSecrets(connectorName, baseUuid);
        // only keep secrets present in incoming config
        upstreamSecrets.keySet().retainAll(incomingSecrets.keySet());

        // get new secrets from the config, if any
        Map<String, String> newSecrets =
                incomingSecrets.entrySet().stream()
                        .filter(entry -> !isSecretStorageReference(secretProviderAlias, connectorName, baseUuid,
                                entry.getKey(), entry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        String newUuid = null;
        if (!newSecrets.isEmpty()) {
            upstreamSecrets.putAll(newSecrets);
            newUuid = saveSecrets(connectorName, upstreamSecrets);
            incomingConfigs.put(SECRET_BUNDLE_ID, newUuid);
            requestContext.setProperty(REQUEST_PROPERTY_NEW_UUID, newUuid);
        } else if (incomingSecrets.isEmpty()) {
            if (baseUuid != null) {
                // empty secrets means the user explicitly emptied the secret property config,
                // telling the response filter to delete all secret bundles
                requestContext.setProperty(REQUEST_PROPERTY_DELETE_ALL_SECRETS, true);
                incomingConfigs.remove(SECRET_BUNDLE_ID);
            }
        }


        // replace secret config values with secret storage references
        Map<String, String> filteredConfigs = redactSecretConfigs(secretProviderAlias,
                connectorName,
                newUuid,
                incomingConfigs
        );

        // save the filtered config back into the request body
        requestContent.updateConfig(filteredConfigs);
        Map<String, ?> finalRequestBody = requestContent.toRequestBody();
        setRequestBody(requestContext, finalRequestBody);
    }

    /**
     * <pre>
     * response filter - handles the DELETE /{connector}/ endpoint, and, in case of edit(PUT) or
     * create(POST) request failure, reverts the changes made to the secret storage in the request
     * filter.
     * Only 4xx HTTP response codes are considered a failure.
     * Other non-success statuses such as 500 INTERNAL_SERVER_ERROR are too generic
     * and might occur after the actual request was successfully processed.
     * </pre>
     *
     * @param requestContext  requestContext, read-only
     * @param responseContext responseContext
     */
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        ConnectRequestType requestType = ConnectRequestContent.getRequestType(requestContext);
        String connectorName = (String) requestContext.getProperty(REQUEST_PROPERTY_CONNECTOR_NAME);
        if (!ALLOWED_REQUEST_TYPES.contains(requestType)) {
            return;
        }

        Optional<String> baseUuid =
                Optional.ofNullable((String) requestContext.getProperty(REQUEST_PROPERTY_BASE_UUID));

        if (requestType.equals(ConnectRequestType.DELETE)) {
            if (ConnectRequestType.requestFailed(requestType, responseContext.getStatus()) || !baseUuid.isPresent()) {
                return;
            }
            // on successful delete, cleanup all secret storage versions for this connector
            try {
                LOGGER.debug(
                        "[{}][{}]: {}",
                        connectorName,
                        baseUuid.get(),
                        "Delete request succeeded, deleting secrets belonging to connector.");
                secretStorage.deleteSecretsAndPreviousSecrets(connectorName, baseUuid.get());
            } catch (Exception e) {
                // best effort, don't rethrow
                LOGGER.warn(
                        "[{}][{}]: Failed cleaning up secrets belonging to deleted connector.",
                        connectorName,
                        baseUuid.orElse(null),
                        e);
            }
        } else if (requestType.equals(ConnectRequestType.CREATE)
                || requestType.equals(ConnectRequestType.EDIT)) {
            Optional<String> newUuid =
                    Optional.ofNullable((String) requestContext.getProperty(REQUEST_PROPERTY_NEW_UUID));

            // on client error, clean up the new secret bundle id
            if (responseContext.getStatusInfo().getFamily().equals(Status.Family.CLIENT_ERROR)) {
                if (newUuid.isPresent()) {
                    LOGGER.debug(
                            "[{}]: Request failed, deleting the secret storage version corresponding to this request.",
                            connectorName);
                    secretStorage.deleteSecrets(connectorName, newUuid.get());
                }
                return;
            }

            // on successful create/edit, mark the request for completion
            // and delete all unused secret storage versions
            if (!ConnectRequestType.requestFailed(requestType, responseContext.getStatus())) {
                String cleanupMsg = "Request succeeded, cleaning up unused secret storage versions.";
                if (newUuid.isPresent()) {
                    LOGGER.debug("[{}]: {}", cleanupMsg, connectorName);
                    secretStorage.markForCompletionAndDeletePrevious(connectorName, newUuid.get());
                } else if (baseUuid.isPresent()) {
                    // uuid stayed the same - do prophylactic cleanup of previous bundles, in case any got stuck
                    LOGGER.debug("[{}]: {}", connectorName, cleanupMsg);
                    Optional<Boolean> deleteAllSecrets = Optional.ofNullable(
                            (Boolean) requestContext.getProperty(REQUEST_PROPERTY_DELETE_ALL_SECRETS));
                    if (deleteAllSecrets.isPresent() && deleteAllSecrets.get()) {
                        // all secrets were removed from the config by the user
                        secretStorage.deleteSecretsAndPreviousSecrets(connectorName, baseUuid.get());
                    } else {
                        secretStorage.deletePreviousSecrets(connectorName, baseUuid.get());
                    }
                }
            }
        }
    }

    private Map<String, String> getUpstreamSecrets(String connectorName, String uuid) {
        if (uuid == null || uuid.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, String> secrets = secretStorage.getSecrets(connectorName, uuid, false);
        if (secrets == null) {
            String msg = "Failed fetching secrets from the secret storage.";
            LOGGER.error("[{}]: {}", connectorName, msg);
            throw new ConnectRestException(Status.INTERNAL_SERVER_ERROR, msg);
        }
        return secrets;
    }

    private String saveSecrets(String connectorName, Map<String, String> secrets) {
        try {
            return secretStorage.saveSecrets(connectorName, secrets);
        } catch (ConcurrentModificationException e) {
            String msg =
                    String.format("Detected concurrent modification, request aborted. %s", e.getMessage());
            LOGGER.error("[{}]: {}", connectorName, msg);
            throw new ConnectRestException(Status.CONFLICT, msg, e);
        }
    }

    static class SecretStorageReference {
        /**
         * no optional groups, each group must not contain any of following chars: ^}:]
         */
        public static final Pattern PATTERN =
                Pattern.compile("\\$\\{(?<provider>[^}:]+?):(?<path>([^}:]+?)):(?<property>[^}]+?)}");
        public static final String TEMPLATE = "${%s:%s:%s}";

        private final String providerAlias;
        private final BundlePath path;
        private final String property;

        public SecretStorageReference(String providerAlias, BundlePath bundlePath, String property) {
            this.providerAlias = providerAlias;
            this.path = bundlePath;
            this.property = property;
        }

        public static SecretStorageReference readReference(String referenceString, String providerAlias)
                throws IllegalArgumentException {
            try {
                Matcher m = PATTERN.matcher(referenceString);
                if (m.matches()) {
                    String configProvider = m.group("provider");
                    String path = m.group("path");
                    String property = m.group("property");
                    if (!configProvider.equals(providerAlias)) {
                        throw new IllegalArgumentException("Not a secret storage reference.");
                    }
                    return new SecretStorageReference(providerAlias, BundlePath.parseFromPath(path), property);
                } else {
                    throw new IllegalArgumentException();
                }
            } catch (Exception e) {
                String msg = String.format("Not a valid config reference: %s. %s", referenceString, e.getMessage());
                throw new IllegalArgumentException(msg);
            }
        }

        public static boolean isSecretStorageReference(String providerAlias, String connectorName, String id,
                                                       String configKey, String configValue) {
            try {
                SecretStorageReference reference = SecretStorageReference.readReference(configValue, providerAlias);
                return reference.path.equals(new BundlePath(connectorName, id))
                        && reference.property.equals(configKey);
            } catch (IllegalArgumentException e) {
                return false;
            }
        }

        public String getConfigProvider() {
            return providerAlias;
        }

        public BundlePath getPath() {
            return path;
        }

        public String getProperty() {
            return property;
        }

        public String toReferenceString() {
            return String.format(TEMPLATE, providerAlias, path.toPath(), property);
        }

        @Override
        public String toString() {
            return toReferenceString();
        }
    }

    static class ConnectRequestContent {
        private final ConnectRequestType requestType;
        private final String connectorName;
        private Map<String, String> config;

        public ConnectRequestContent(
                String connectorName, Map<String, String> config, ConnectRequestType requestType) {
            this.config = config;
            this.connectorName = connectorName;
            this.requestType = requestType;
        }

        public static ConnectRequestContent parseFromRequestContext(ContainerRequestContext requestContext) {
            ConnectRequestType requestType = getRequestType(requestContext);
            String requestPath = requestContext.getUriInfo().getPath();
            if (requestType.equals(ConnectRequestType.DELETE)) {
                String connectorName = getConnectorName(requestType, null, requestPath);
                return new ConnectRequestContent(connectorName, null, requestType);
            }
            InputStream inputStream = requestContext.getEntityStream();
            byte[] inBytes = getRequestBodyBytes(inputStream);
            requestContext.setEntityStream(new ByteArrayInputStream(inBytes));
            Map<String, ?> requestBody = extractRequestBody(inBytes);
            String connectorName =
                    getConnectorName(requestType, requestBody, requestContext.getUriInfo().getPath());
            Map<String, String> connectorConfigs = getConnectorConfigs(requestType, requestBody);
            return new ConnectRequestContent(connectorName, connectorConfigs, requestType);
        }

        private static Map<String, ?> extractRequestBody(byte[] in) throws ConnectRestException {
            try {
                return OBJECT_MAPPER.readValue(in, new TypeReference<Map<String, ?>>() {
                });
            } catch (IOException e) {
                String msg = "Failed parsing request body. " + e.getMessage();
                LOGGER.error(msg);
                throw new ConnectRestException(Status.BAD_REQUEST, msg);
            }
        }

        private static byte[] getRequestBodyBytes(InputStream in) {
            byte[] bytes = inputStreamToByteArray(in);
            if (bytes == null) {
                String msg = "Failed parsing input stream to byte array.";
                LOGGER.error(msg);
                throw new ConnectRestException(Status.INTERNAL_SERVER_ERROR, msg);
            }
            return bytes;
        }

        static ConnectRequestType getRequestType(ContainerRequestContext requestContext) {
            String method = requestContext.getMethod();
            String path = requestContext.getUriInfo().getPath();
            List<ConnectRequestType> filterableRequestTypes =
                    Arrays.asList(ConnectRequestType.CREATE, ConnectRequestType.EDIT, ConnectRequestType.DELETE);
            Optional<ConnectRequestType> requestType =
                    filterableRequestTypes.stream()
                            .filter(type -> type.matchesRequestType(method, path))
                            .findFirst();
            return requestType.orElse(ConnectRequestType.OTHER);
        }

        private static String getConnectorName(
                ConnectRequestType requestType, Map<String, ?> requestBody, String requestPath)
                throws ConnectRestException {
            String connectorName =
                    requestType.equals(ConnectRequestType.CREATE)
                            ? (String) requestBody.get("name")
                            : extractConnectorNameFromPath(requestPath);
            if (connectorName == null || connectorName.isEmpty()) {
                String msg = "Failed parsing connector name from the request.";
                LOGGER.error(msg);
                throw new ConnectRestException(Status.INTERNAL_SERVER_ERROR, msg);
            }
            return connectorName;
        }

        private static Map<String, String> getConnectorConfigs(ConnectRequestType requestType, Map<String, ?> requestBody)
                throws ConnectRestException {
            Map<String, String> configs = null;
            try {
                switch (requestType) {
                    case CREATE:
                        Object configNode = requestBody.get("config");
                        if (configNode instanceof Map) {
                            configs = OBJECT_MAPPER.convertValue(configNode, new TypeReference<Map<String, String>>() {
                            });
                        }
                        break;
                    case EDIT:
                        configs = requestBody.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, value -> (String) value.getValue()));
                        break;
                    default:
                        String msg = String.format("Invalid request type: %s.", requestType.name());
                        LOGGER.error(msg);
                        throw new ConnectRestException(Status.INTERNAL_SERVER_ERROR, msg);
                }
                if (configs == null || configs.isEmpty()) {
                    throw new IllegalArgumentException();
                }
            } catch (ConnectRestException e) {
                throw e;
            } catch (Exception e) {
                String msg = "Failed parsing connector config from the request. " + e.getMessage();
                LOGGER.error(msg);
                throw new ConnectRestException(Status.BAD_REQUEST, msg);
            }
            return configs;
        }

        public ConnectRequestType getRequestType() {
            return requestType;
        }

        public String getConnectorName() {
            return connectorName;
        }

        public Map<String, String> getConfig() {
            return config;
        }

        public void updateConfig(Map<String, String> config) {
            this.config = new HashMap<>(config);
        }

        public Map<String, ?> toRequestBody() {
            if (requestType.equals(ConnectRequestType.CREATE)) {
                Map<String, Object> requestBody = new HashMap<>();
                requestBody.put("name", connectorName);
                requestBody.put("config", config);
                return requestBody;
            } else if (requestType.equals(ConnectRequestType.EDIT)) {
                return config;
            } else {
                return null;
            }
        }

        @Override
        public String toString() {
            return toRequestBody().toString();
        }
    }
}
