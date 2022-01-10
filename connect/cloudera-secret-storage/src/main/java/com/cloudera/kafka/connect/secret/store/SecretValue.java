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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties
public final class SecretValue {
    private final String connector;
    private final String id;
    private final Map<String, String> secrets;

    @JsonCreator
    public SecretValue(@JsonProperty("connector") String connector, @JsonProperty("id") String id,
                       @JsonProperty("secrets") Map<String, String> secrets) {
        this.connector = Objects.requireNonNull(connector);
        this.id = Objects.requireNonNull(id);
        this.secrets = Objects.requireNonNull(secrets);
    }

    public String getConnector() {
        return connector;
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getSecrets() {
        return secrets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecretValue that = (SecretValue) o;
        return connector.equals(that.connector) && id.equals(that.id) && secrets.equals(that.secrets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connector, id, secrets);
    }

    @Override
    public String toString() {
        return "SecretValue{" +
                "connector='" + connector + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
