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

package com.cloudera.kafka.connect.rest.authorization.extension;

import com.cloudera.kafka.connect.authorization.AuthorizableAction;
import com.cloudera.kafka.connect.authorization.ConnectAuthorizer;
import com.cloudera.kafka.connect.authorization.Operation;
import com.cloudera.kafka.connect.authorization.Resource;
import com.cloudera.kafka.connect.authorization.ResourceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.cloudera.kafka.connect.authorization.Resource.clusterResource;
import static com.cloudera.kafka.connect.authorization.Resource.connectorResource;

/**
 * Authorize all simple resource requests. Requests are pre-checked, with the exception of resource
 * listings, which are post-processed to filter non-authorized elements from the response.
 *
 * If new endpoint is required then introduction of a new pattern constant is needed. The new
 * constant has to be added to the authorizationMappings with the http method, resource type
 * and operation mapper properties. If response authorization required then the second filter
 * and the {@link #isResponseFilterable} method needs to be updated explicitly.
 */
@Priority(Priorities.AUTHORIZATION)
public class ConnectAuthorizationFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectAuthorizationFilter.class);

    private static final Principal ANONYMOUS_PRINCIPAL = new SimplePrincipal(ConnectAuthorizer.ANONYMOUS_PRINCIPAL_NAME);

    private static final String EXPANSION_QUERY_PARAM = "expand";

    private static final Pattern ROOT_REQUEST_PATTERN = Pattern.compile("^$");
    private static final Pattern CONNECTOR_PLUGIN_REQUEST_PATTERN = Pattern.compile("^connector-plugins[/]?");
    private static final Pattern CONNECTOR_PLUGIN_CONFIG_REQUEST_PATTERN = Pattern.compile("^connector-plugins/[^/]+/config[/]?");
    private static final Pattern CONNECTOR_PLUGIN_VALIDATE_REQUEST_PATTERN = Pattern.compile("^connector-plugins/[^/]+/config/validate[/]?");
    private static final Pattern LOGGER_REQUEST_PATTERN = Pattern.compile("^admin/loggers([/]?|/[^/]+)[/]?");
    private static final Pattern CONNECTOR_LIST_OR_CREATE_PATTERN = Pattern.compile("^connectors[/]?");
    private static final Pattern CONNECTOR_RESOURCE_PATTERN = Pattern.compile("^connectors/[^/]+[/]?");
    private static final Pattern CONNECTOR_CONFIG_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/config[/]?");
    private static final Pattern CONNECTOR_TASK_CONFIG_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/tasks-config[/]?");
    private static final Pattern CONNECTOR_STATUS_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/status[/]?");
    private static final Pattern CONNECTOR_TOPICS_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/topics[/]?");
    private static final Pattern CONNECTOR_TOPICS_RESET_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/topics/reset[/]?");
    private static final Pattern CONNECTOR_MANAGE_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/(pause|resume)[/]?");
    private static final Pattern CONNECTOR_RESTART_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/restart[/]?");
    private static final Pattern CONNECTOR_TASK_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/tasks[/]?");
    private static final Pattern CONNECTOR_TASK_STATUS_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/tasks/[^/]+/status[/]?");
    private static final Pattern CONNECTOR_TASK_RESTART_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/tasks/[^/]+/restart[/]?");
    private static final Pattern CONNECTOR_PERMISSIONS_REQUEST_PATTERN = Pattern.compile("^connector-permissions[/]?");
    private static final Pattern CONNECTOR_FENCE_REQUEST_PATTERN = Pattern.compile("^connectors/[^/]+/fence[/]?");

    private static final List<AuthorizationMapping> AUTHORIZATION_MAPPINGS;
    static {
        List<AuthorizationMapping> mappings = new ArrayList<>();
        mappings.add(new AuthorizationMapping(HttpMethod.POST, CONNECTOR_LIST_OR_CREATE_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.CREATE)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, CONNECTOR_RESOURCE_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.VIEW)));
        mappings.add(new AuthorizationMapping(HttpMethod.DELETE, CONNECTOR_RESOURCE_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.DELETE)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, CONNECTOR_CONFIG_REQUEST_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.VIEW)));
        mappings.add(new AuthorizationMapping(HttpMethod.PUT, CONNECTOR_CONFIG_REQUEST_PATTERN, ResourceType.CONNECTOR, new ConnectorExistsConditionalMapper(Operation.EDIT, Operation.CREATE)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, ROOT_REQUEST_PATTERN, ResourceType.CLUSTER, new StaticOperationMapper(Operation.VIEW)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, CONNECTOR_PLUGIN_REQUEST_PATTERN, ResourceType.CLUSTER, new StaticOperationMapper(Operation.VIEW)));
        mappings.add(new AuthorizationMapping(HttpMethod.PUT, CONNECTOR_PLUGIN_VALIDATE_REQUEST_PATTERN, ResourceType.CLUSTER, new StaticOperationMapper(Operation.VALIDATE)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, LOGGER_REQUEST_PATTERN, ResourceType.CLUSTER, new StaticOperationMapper(Operation.MANAGE)));
        mappings.add(new AuthorizationMapping(HttpMethod.PUT, LOGGER_REQUEST_PATTERN, ResourceType.CLUSTER, new StaticOperationMapper(Operation.MANAGE)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, CONNECTOR_TASK_CONFIG_REQUEST_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.VIEW)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, CONNECTOR_STATUS_REQUEST_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.VIEW)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, CONNECTOR_TOPICS_REQUEST_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.VIEW)));
        mappings.add(new AuthorizationMapping(HttpMethod.PUT, CONNECTOR_TOPICS_RESET_REQUEST_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.MANAGE)));
        mappings.add(new AuthorizationMapping(HttpMethod.PUT, CONNECTOR_MANAGE_REQUEST_PATTERN, ResourceType.CONNECTOR,  new StaticOperationMapper(Operation.MANAGE)));
        mappings.add(new AuthorizationMapping(HttpMethod.POST, CONNECTOR_RESTART_REQUEST_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.MANAGE)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, CONNECTOR_TASK_REQUEST_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.VIEW)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, CONNECTOR_TASK_STATUS_REQUEST_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.VIEW)));
        mappings.add(new AuthorizationMapping(HttpMethod.POST, CONNECTOR_TASK_RESTART_REQUEST_PATTERN, ResourceType.CONNECTOR, new StaticOperationMapper(Operation.MANAGE)));
        mappings.add(new AuthorizationMapping(HttpMethod.GET, CONNECTOR_PLUGIN_CONFIG_REQUEST_PATTERN, ResourceType.CLUSTER, new StaticOperationMapper(Operation.VIEW)));
        AUTHORIZATION_MAPPINGS = Collections.unmodifiableList(mappings);
    }

    private final ConnectAuthorizer authorizer;
    private final ConnectClusterState clusterState;
    private final String superUserPrincipalName;

    public ConnectAuthorizationFilter(ConnectAuthorizer authorizer, ConnectClusterState clusterState,
                                      String superUserPrincipalName) {
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer must not be null");
        this.clusterState = Objects.requireNonNull(clusterState, "clusterState must not be null");
        this.superUserPrincipalName = Objects.requireNonNull(superUserPrincipalName, "superUserPrincipalName must not be null");
    }

    private static Response errorResponse(Status status, String message) {
        return Response.status(status).entity(new ErrorMessage(status.getStatusCode(), message)).build();
    }

    private static Response errorResponse(Status status) {
        return errorResponse(status, status.getReasonPhrase());
    }

    private static boolean isAnonymous(ContainerRequestContext requestContext) {
        return requestContext.getSecurityContext().getUserPrincipal() == null
                || requestContext.getSecurityContext().getUserPrincipal().getName() == null;
    }

    private static Principal getPrincipal(ContainerRequestContext requestContext) {
        if (isAnonymous(requestContext)) {
            return ANONYMOUS_PRINCIPAL;
        }
        return requestContext.getSecurityContext().getUserPrincipal();
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        Principal principal = getPrincipal(requestContext);
        LOG.debug("Authorize request for principal {}.", principal.getName());

        if (isSuperUser(principal.getName())) {
            LOG.debug("Skipping request authorization for super user principal.");
            return;
        }

        if (isInternalEndpoint(requestContext)) {
            LOG.warn("Request sent to internal endpoint by non super user principal.");
            requestContext.abortWith(errorResponse(Status.FORBIDDEN));
            return;
        }

        if (isRequestFilterSkippable(requestContext)) {
            return;
        }

        if (isResponseFilterable(requestContext)) {
            LOG.debug("Skipping request authorization since this request has to be validated after possible response is ready.");
            return;
        }

        String uriPath = requestContext.getUriInfo().getPath();
        String method = requestContext.getMethod();

        AuthActionOrErrorStatus authActionOrErrorStatus = getAuthActionOrErrorStatus(requestContext);
        if (authActionOrErrorStatus.getStatus().isPresent()) {
            LOG.error("Error during authorization action detection uri: {} method: {} response status: {}", uriPath, method, authActionOrErrorStatus.getStatus().get());
            requestContext.abortWith(errorResponse(authActionOrErrorStatus.getStatus().get()));
            return;
        }

        AuthorizableAction originalAuthorizableAction = authActionOrErrorStatus.getAction()
            .orElseThrow(() -> new IllegalStateException("Request authorization failed because authorizable action is missing!"));

        // The optional view operation is required in some cases since the error response (forbidden or not_found) is based on view permission.
        Optional<AuthorizableAction> optionalViewAction = getOptionalViewOperation(requestContext, originalAuthorizableAction);
        if (optionalViewAction.isPresent()) {
            AuthorizableAction viewAction = optionalViewAction.get();
            Set<AuthorizableAction> authorizedActions = authorizer.filterAuthorized(principal, new HashSet<>(Arrays.asList(originalAuthorizableAction, viewAction)));

            if (!authorizedActions.contains(originalAuthorizableAction)) {
                LOG.debug("Request authorization failed.");
                Status responseStatus = authorizedActions.contains(viewAction) ? Status.FORBIDDEN : Status.NOT_FOUND;
                String responseMessage = responseStatus.getReasonPhrase();
                if (originalAuthorizableAction.getResource().getResourceType() == ResourceType.CONNECTOR
                        && responseStatus == Status.NOT_FOUND) {
                    responseMessage = "Unknown connector " + originalAuthorizableAction.getResource().getResourceName();
                }
                requestContext.abortWith(errorResponse(responseStatus, responseMessage));
                return;
            }
        } else {
            if (!authorizer.isAuthorized(principal, originalAuthorizableAction)) {
                LOG.debug("Request authorization failed.");
                Status responseStatus = Status.NOT_FOUND;
                String responseMessage = responseStatus.getReasonPhrase();
                if (originalAuthorizableAction.getResource().getResourceType() == ResourceType.CONNECTOR) {
                    responseMessage = "Unknown connector " + originalAuthorizableAction.getResource().getResourceName();
                }
                requestContext.abortWith(errorResponse(responseStatus, responseMessage));
                return;
            }
        }

        LOG.debug("Request authorization succeeded.");
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        Principal principal = getPrincipal(requestContext);
        LOG.debug("Authorize response for principal {}.", principal.getName());

        if (isSuperUser(principal.getName())) {
            LOG.debug("Skipping response authorization for super user principal.");
            return;
        }

        if (isResponseFilterSkippable(requestContext, responseContext)) {
            return;
        }

        MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
        ConnectorListResponseHandler responseHandler = (queryParams != null && queryParams.containsKey(EXPANSION_QUERY_PARAM)) ?
            new ExpandedConnectorListResponseHandler(responseContext) :
            new UnexpandedConnectorListResponseHandler(responseContext);

        Set<String> connectorNames = responseHandler.getConnectorNames();
        if (connectorNames.isEmpty()) {
            LOG.debug("Response authorization not required - empty response.");
            return;
        }

        Set<AuthorizableAction> authorizableActionSet = connectorNames.stream()
            .map(name -> new AuthorizableAction(connectorResource(name), Operation.VIEW, true, false))
            .collect(Collectors.toSet());

        Set<AuthorizableAction> authorizedActions = authorizer.filterAuthorized(principal, authorizableActionSet);
        responseContext.setEntity(responseHandler.getAuthorizedResponseEntity(authorizedActions));

        LOG.debug("Response authorization succeeded.");
    }

    private boolean isRequestFilterSkippable(ContainerRequestContext request) {
        String uriPath = request.getUriInfo().getPath();
        String method = request.getMethod();

        if (CONNECTOR_PERMISSIONS_REQUEST_PATTERN.matcher(uriPath).matches() && HttpMethod.GET.equals(method)) {
            LOG.debug("Skipping request authorization since authorization is not needed.");
            return true;
        }

        return false;
    }

    private boolean isResponseFilterSkippable(ContainerRequestContext request, ContainerResponseContext response) {
        if (!isResponseFilterable(request)) {
            LOG.debug("Skipping response authorization since the request is already validated.");
            return true;
        }

        if (response.getStatus() != 200) {
            LOG.debug("Skipping response authorization since the response status is not OK (HTTP 200).");
            return true;
        }

        return false;
    }

    private boolean isResponseFilterable(ContainerRequestContext request) {
        return CONNECTOR_LIST_OR_CREATE_PATTERN.matcher(request.getUriInfo().getPath()).matches() &&
            HttpMethod.GET.equals(request.getMethod());
    }

    private boolean isInternalEndpoint(ContainerRequestContext request) {
        return (CONNECTOR_TASK_REQUEST_PATTERN.matcher(request.getUriInfo().getPath()).matches()
            && HttpMethod.POST.equals(request.getMethod()))
            || (CONNECTOR_FENCE_REQUEST_PATTERN.matcher(request.getUriInfo().getPath()).matches()
            && HttpMethod.PUT.equals(request.getMethod()));
    }

    private boolean isSuperUser(String principalName) {
        return superUserPrincipalName.equals(principalName);
    }

    private AuthActionOrErrorStatus getAuthActionOrErrorStatus(ContainerRequestContext request) {
        return AUTHORIZATION_MAPPINGS.stream()
            .map(am -> am.mapAuthActionOrErrorStatus(request, clusterState))
            .filter(Objects::nonNull)
            .findFirst()
            .orElseGet(() -> {
                LOG.error("Request uri path {} unknown from the Cloudera authorization extension point of view.", request.getUriInfo().getPath());
                return new AuthActionOrErrorStatus(Status.INTERNAL_SERVER_ERROR);
            });
    }

    private Optional<AuthorizableAction> getOptionalViewOperation(ContainerRequestContext request, AuthorizableAction action) {
        if (action.getOperation().equals(Operation.VIEW) || LOGGER_REQUEST_PATTERN.matcher(request.getUriInfo().getPath()).matches()) {
            return Optional.empty();
        }
        return Optional.of(new AuthorizableAction(action.getResource(), Operation.VIEW, false, false));
    }

    private static class AuthorizationMapping {
        private final String httpMethod;
        private final Pattern pattern;
        private final ResourceType resourceType;
        private final OperationMapper operationMapper;

        private AuthorizationMapping(String httpMethod, Pattern pattern, ResourceType resourceType, OperationMapper operationMapper) {
            if (!operationMapper.isResourceTypeSupported(resourceType)) {
                throw new IllegalStateException("Authorization resource type and operation are not compatible.");
            }

            this.resourceType = resourceType;
            this.pattern = pattern;
            this.httpMethod = httpMethod;
            this.operationMapper = operationMapper;
        }

        public AuthActionOrErrorStatus mapAuthActionOrErrorStatus(ContainerRequestContext request, ConnectClusterState clusterState) {
            String uriPath = request.getUriInfo().getPath();
            String method = request.getMethod();
            if (!pattern.matcher(uriPath).matches() || !httpMethod.equals(method)) {
                return null;
            }

            Resource resource;
            switch (resourceType) {
                case CLUSTER:
                    resource = clusterResource();
                    break;
                case CONNECTOR: {
                    String connectorName = getRequestConnectorName(request);
                    if (connectorName == null || connectorName.isEmpty()) {
                        return new AuthActionOrErrorStatus(Status.BAD_REQUEST);
                    }
                    resource = connectorResource(connectorName);
                    break;
                }
                default:
                    LOG.error("Could not extract authorizable resource from uri: {} method: {}", uriPath, method);
                    return new AuthActionOrErrorStatus(Status.INTERNAL_SERVER_ERROR);
            }

            Operation operation = operationMapper.getOperation(resource, clusterState);
            if (operation == null) {
                LOG.error("Could not extract authorizable operation from uri: {} method: {} resource: {}", uriPath, method, resource);
                return new AuthActionOrErrorStatus(Status.INTERNAL_SERVER_ERROR);
            }

            return new AuthActionOrErrorStatus(new AuthorizableAction(resource, operation));
        }

        /**
         * All simple endpoints connector uri starts with "/connectors/" and after that the connector
         * name comes. The connector name ends with a "/" or not (if the path ends with the name)
         * furthermore the remaining parts of the path are not relevant.
         */
        private String getRequestConnectorName(ContainerRequestContext request) {
            String uriPath = request.getUriInfo().getPath();
            if (CONNECTOR_LIST_OR_CREATE_PATTERN.matcher(uriPath).matches() && HttpMethod.POST.equals(request.getMethod())) {
                return getConnectorNameFromCreateConnectorRequestBody(request);
            }

            String subPath = uriPath.replaceFirst("connectors/", "");
            int position = subPath.indexOf('/');
            return position == -1 ? subPath : subPath.substring(0, position);
        }

        private String getConnectorNameFromCreateConnectorRequestBody(ContainerRequestContext request) {
            try {
                byte[] content;
                try (InputStream inputStream = request.getEntityStream()) {
                    content = readStream(inputStream);
                    if (content == null) {
                        LOG.error("POST connector creation request with invalid body.");
                        return null;
                    }
                }
                request.setEntityStream(new ByteArrayInputStream(content));

                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonMap = mapper.readTree(content);
                JsonNode name = jsonMap.get("name");
                return name == null || !name.isTextual() ? null : name.asText().trim();
            } catch (IOException e) {
                LOG.error("POST connector creation request with invalid body.");
                return null;
            }
        }

        private byte[] readStream(InputStream inputStream) {
            byte[] buffer = new byte[1024];
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                int readLen;
                while ((readLen = inputStream.read(buffer)) != -1) {
                    bos.write(buffer, 0, readLen);
                }
                return bos.toByteArray();
            } catch (IOException e) {
                LOG.error("Error reading request body", e);
                return null;
            }
        }
    }

    private interface OperationMapper {
        Operation getOperation(Resource resource, ConnectClusterState clusterState);
        boolean isResourceTypeSupported(ResourceType resourceType);
    }

    private static class StaticOperationMapper implements OperationMapper {
        private final Operation operation;

        private StaticOperationMapper(Operation operation) {
            this.operation = operation;
        }

        @Override
        public Operation getOperation(Resource resource, ConnectClusterState clusterState) {
            return operation;
        }

        @Override
        public boolean isResourceTypeSupported(ResourceType resourceType) {
            return resourceType.getOperations().contains(operation);
        }
    }

    private static class ConnectorExistsConditionalMapper implements OperationMapper {
        private final Operation operationIfExists;
        private final Operation operationIfNotExists;

        private ConnectorExistsConditionalMapper(Operation operationIfExists, Operation operationIfNotExists) {
            this.operationIfExists = operationIfExists;
            this.operationIfNotExists = operationIfNotExists;
        }

        @Override
        public Operation getOperation(Resource resource, ConnectClusterState clusterState) {
            if (!ResourceType.CONNECTOR.equals(resource.getResourceType())) {
                return null;
            }

            return clusterState.connectors().contains(resource.getResourceName()) ? operationIfExists : operationIfNotExists;
        }

        @Override
        public boolean isResourceTypeSupported(ResourceType resourceType) {
            return ResourceType.CONNECTOR.equals(resourceType) &&
                ResourceType.CONNECTOR.getOperations().contains(operationIfExists) &&
                ResourceType.CONNECTOR.getOperations().contains(operationIfNotExists);
        }
    }

    private static class AuthActionOrErrorStatus {
        private final Optional<AuthorizableAction> action;
        private final Optional<Status> status;

        private AuthActionOrErrorStatus(AuthorizableAction action) {
            this.action = Optional.of(action);
            status = Optional.empty();

        }

        private AuthActionOrErrorStatus(Status status) {
            this.status = Optional.of(status);
            action = Optional.empty();
        }

        public Optional<AuthorizableAction> getAction() {
            return action;
        }

        public Optional<Status> getStatus() {
            return status;
        }
    }

    private interface ConnectorListResponseHandler {
        Set<String>  getConnectorNames();
        Object getAuthorizedResponseEntity(Set<AuthorizableAction> authorizedActions);
    }

    private static class UnexpandedConnectorListResponseHandler implements ConnectorListResponseHandler {
        private final Collection<String> response;

        private UnexpandedConnectorListResponseHandler(ContainerResponseContext response) {
            this.response = getResponseCollection(response);
        }

        @Override
        public Set<String> getConnectorNames() {
            return new HashSet<>(response);
        }

        @Override
        public Collection<String> getAuthorizedResponseEntity(Set<AuthorizableAction> authorizedActions) {
            return authorizedActions.stream()
                .map(action -> action.getResource().getResourceName())
                .collect(Collectors.toSet());
        }

        private Collection<String> getResponseCollection(ContainerResponseContext response) {
            Object entity = response.getEntity();
            if (entity instanceof Collection) {
                @SuppressWarnings("unchecked") Collection<String> outCollection = (Collection<String>) entity;
                return outCollection;
            } else {
                throw new IllegalStateException("Response entity is not an instance of collection.");
            }
        }
    }

    private static class ExpandedConnectorListResponseHandler implements ConnectorListResponseHandler {
        private final Map<String, ?> response;

        private ExpandedConnectorListResponseHandler(ContainerResponseContext responseContext) {
            response = getResponseMap(responseContext);
        }

        @Override
        public Set<String> getConnectorNames() {
            return new HashSet<>(response.keySet());
        }

        @Override
        public Map<String, ?> getAuthorizedResponseEntity(Set<AuthorizableAction> authorizedActions) {
            response.keySet().retainAll(getAuthorizedConnectorNamesBasedOnExpand(authorizedActions));
            return response;
        }

        private Map<String, ?> getResponseMap(ContainerResponseContext response) {
            Object entity = response.getEntity();
            if (entity instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, ?> outMap = (Map<String, ?>) entity;
                return outMap;
            } else {
                throw new IllegalStateException("Response entity is not an instance of map.");
            }
        }

        private Set<String> getAuthorizedConnectorNamesBasedOnExpand(Set<AuthorizableAction> actions) {
            return actions.stream()
                    .filter(action -> Operation.VIEW.equals(action.getOperation()))
                    .map(action -> action.getResource().getResourceName())
                    .collect(Collectors.toSet());
        }
    }

    /**
     * Same structure as org.apache.kafka.connect.runtime.rest.entities.ErrorMessage.
     * org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper produces error responses using this
     * structure, the filter should also use it in order for Connect Workers to be able to deserialize responses.
     */
    private static class ErrorMessage {
        private final int errorCode;
        private final String message;

        @JsonCreator
        public ErrorMessage(@JsonProperty("error_code") int errorCode, @JsonProperty("message") String message) {
            this.errorCode = errorCode;
            this.message = message;
        }

        @JsonProperty("error_code")
        public int errorCode() {
            return errorCode;
        }

        @JsonProperty
        public String message() {
            return message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ErrorMessage that = (ErrorMessage) o;
            return Objects.equals(errorCode, that.errorCode) &&
                    Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(errorCode, message);
        }
    }

    private static class SimplePrincipal implements Principal {
        private final String name;

        SimplePrincipal(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
