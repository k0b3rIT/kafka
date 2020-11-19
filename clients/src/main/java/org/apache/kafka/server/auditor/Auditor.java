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

package org.apache.kafka.server.auditor;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An auditor class that can be used to hook into the request after its completion and do auditing tasks, such as
 * logging to a special file or sending information about the request to external systems.
 * Threading model:
 * <ul>
 *     <li>The auditor implementation must be thread-safe.</li>
 *     <li>The auditor implementation is expected to be asynchronous with low latency as it is used in performance
 *     sensitive areas, such as in handling produce requests.</li>
 *     <li>Any threads or thread pools used for processing remote operations asynchronously can be started during
 *     start(). These threads must be shutdown during close().</li>
 * </ul>
 */
@InterfaceStability.Unstable
public interface Auditor extends Configurable, AutoCloseable {

    class AuditInformation {
        private final ResourcePattern resource;
        private final int error;

        /**
         * Creates a new AuthorizationInformation object that contains all authorization related data for
         * auditing. It is assumed that if no error is present then the authorization was successful, otherwise it was
         * unsuccessful or didn't happen and the reason can be extracted from {@link AuditInformation#error}.
         * @param resource is a resource on which authorization was performed.
         * @param error is a specific error that happened. It's 0 if the authorization was successful.
         */
        public AuditInformation(ResourcePattern resource, int error) {
            this.resource = resource;
            this.error = error;
        }

        public ResourcePattern resource() {
            return resource;
        }

        public int error() {
            return error;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AuditInformation that = (AuditInformation) o;
            return Objects.equals(resource, that.resource) &&
                error == that.error;
        }

        @Override
        public int hashCode() {
            return Objects.hash(resource, error);
        }
    }

    /**
     * Called on request completion before returning the response to the client. It allows auditing multiple resources
     * in the request, such as multiple topics being created.
     * @param event is an event object that contains all related entities for the event.
     * @param requestContext contains metadata to the request.
     * @param auditInformation is the operation, resource and the outcome of the authorization for any resource
     *                                in the request together.
     */
    void audit(AuditEvent event, AuthorizableRequestContext requestContext,
               Map<AclOperation, Set<AuditInformation>> auditInformation);
}
