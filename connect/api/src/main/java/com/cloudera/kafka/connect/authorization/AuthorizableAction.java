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

package com.cloudera.kafka.connect.authorization;

import java.util.Objects;

public class AuthorizableAction {
    private final Resource resource;
    private final Operation operation;
    private final boolean logIfAllowed;
    private final boolean logIfDenied;

    public AuthorizableAction(Resource resource, Operation operation, boolean logIfAllowed, boolean logIfDenied) {
        Objects.requireNonNull(resource, "Resource must not be null");
        Objects.requireNonNull(operation, "Operation must not be null");

        this.resource = resource;
        this.operation = operation;
        this.logIfAllowed = logIfAllowed;
        this.logIfDenied = logIfDenied;
    }

    public AuthorizableAction(Resource resource, Operation operation) {
        this(resource, operation, true, true);
    }

    public Resource getResource() {
        return resource;
    }

    public Operation getOperation() {
        return operation;
    }

    public boolean isLogIfAllowed() {
        return logIfAllowed;
    }

    public boolean isLogIfDenied() {
        return logIfDenied;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthorizableAction that = (AuthorizableAction) o;
        return logIfAllowed == that.logIfAllowed && logIfDenied == that.logIfDenied && resource.equals(that.resource) && operation == that.operation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, operation, logIfAllowed, logIfDenied);
    }

    @Override
    public String toString() {
        return "AuthorizableAction{" +
            "resource=" + resource +
            ", operation=" + operation +
            ", logIfAllowed=" + logIfAllowed +
            ", logIfDenied=" + logIfDenied +
            '}';
    }
}
