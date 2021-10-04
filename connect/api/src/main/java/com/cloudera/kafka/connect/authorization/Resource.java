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

public class Resource {
    private static final String CLUSTER_RESOURCE_NAME = "CLUSTER";
    private static final Resource CLUSTER_RESOURCE = new Resource(ResourceType.CLUSTER, CLUSTER_RESOURCE_NAME);
    private final ResourceType resourceType;
    private final String resourceName;

    private Resource(ResourceType resourceType, String resourceName) {
        this.resourceType = resourceType;
        this.resourceName = resourceName;
    }

    public static Resource clusterResource() {
        return CLUSTER_RESOURCE;
    }

    public static Resource connectorResource(String resourceName) {
        if (resourceName == null) {
            throw new NullPointerException("resourceName cannot be null");
        }
        return new Resource(ResourceType.CONNECTOR, resourceName);
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public String getResourceName() {
        return resourceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Resource resource = (Resource) o;
        return resourceType == resource.resourceType && Objects.equals(resourceName, resource.resourceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, resourceName);
    }

    @Override
    public String toString() {
        return "Resource{" +
            "resourceType=" + resourceType +
            ", resourceName='" + resourceName + '\'' +
            '}';
    }
}
