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
package com.cloudera.kafka.connect.secret.store;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

/**
 * Used when marking a specific secret bundle as completed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder(alphabetic = true)
public final class CompletionMarkKey extends RecordKey {
    private final String connector;
    private final String id;

    public CompletionMarkKey(@JsonProperty("connector") String connector, @JsonProperty("id") String id) {
        this.connector = Objects.requireNonNull(connector);
        this.id = Objects.requireNonNull(id);
    }

    public String getConnector() {
        return connector;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompletionMarkKey that = (CompletionMarkKey) o;
        return connector.equals(that.connector) && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connector, id);
    }

    @Override
    public String toString() {
        return "CompletionMarkKey{" +
                "connector='" + connector + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
