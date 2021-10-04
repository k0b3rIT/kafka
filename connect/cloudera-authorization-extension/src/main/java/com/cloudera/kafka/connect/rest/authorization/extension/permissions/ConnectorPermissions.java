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

package com.cloudera.kafka.connect.rest.authorization.extension.permissions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public final class ConnectorPermissions {
    private final boolean createAllowed;
    private final Map<String, Permissions> permissions;

    @JsonCreator
    public ConnectorPermissions(@JsonProperty("createAllowed") boolean createAllowed,
                                @JsonProperty("permissions") Map<String, Permissions> permissions) {
        this.createAllowed = createAllowed;
        this.permissions = permissions;
    }

    @JsonProperty("createAllowed")
    public boolean isCreateAllowed() {
        return createAllowed;
    }

    @JsonProperty("permissions")
    public Map<String, Permissions> getPermissions() {
        return permissions;
    }
}
