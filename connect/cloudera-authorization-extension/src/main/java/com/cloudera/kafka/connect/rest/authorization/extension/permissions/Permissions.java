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

import java.util.Objects;

public final class Permissions {
    private final boolean editable;
    private final boolean deletable;
    private final boolean manageable;

    @JsonCreator
    public Permissions(@JsonProperty("editable") boolean editable,
                       @JsonProperty("deletable") boolean deletable,
                       @JsonProperty("manageable") boolean manageable) {
        this.editable = editable;
        this.deletable = deletable;
        this.manageable = manageable;
    }

    @JsonProperty("editable")
    public boolean isEditable() {
        return editable;
    }

    @JsonProperty("deletable")
    public boolean isDeletable() {
        return deletable;
    }

    @JsonProperty("manageable")
    public boolean isManageable() {
        return manageable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Permissions that = (Permissions) o;
        return editable == that.editable && deletable == that.deletable && manageable == that.manageable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(editable, deletable, manageable);
    }

    @Override
    public String toString() {
        return "Permissions{" +
                "editable=" + editable +
                ", deletable=" + deletable +
                ", manageable=" + manageable +
                '}';
    }
}
