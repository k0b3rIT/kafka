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

import java.util.Objects;

public final class BundlePath {
    private static final String PATH_SEPARATOR = "/";

    private final String connector;
    private final String bundleId;

    public BundlePath(String connector, String bundleId) {
        this.connector = Objects.requireNonNull(connector);
        this.bundleId = Objects.requireNonNull(bundleId);
    }

    /**
     * Parses a connector name and a bundle id from a path.
     * @param path The path to parse from.
     * @return The parsed path or null if the path is invalid.
     */
    public static BundlePath parseFromPath(String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        String[] parts = path.split(PATH_SEPARATOR);
        if (parts.length != 2) {
            return null;
        }
        return new BundlePath(parts[0], parts[1]);
    }

    public String toPath() {
        return connector + PATH_SEPARATOR + bundleId;
    }

    public String getConnector() {
        return connector;
    }

    public String getBundleId() {
        return bundleId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BundlePath that = (BundlePath) o;
        return connector.equals(that.connector) && bundleId.equals(that.bundleId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connector, bundleId);
    }

    @Override
    public String toString() {
        return "BundleReference{" +
                "connector='" + connector + '\'' +
                ", bundleId='" + bundleId + '\'' +
                '}';
    }
}
