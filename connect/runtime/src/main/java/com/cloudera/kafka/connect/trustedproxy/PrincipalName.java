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
package com.cloudera.kafka.connect.trustedproxy;

import java.util.Objects;

public class PrincipalName {
    private final String primary;
    private final String instance;
    private final String realm;

    public PrincipalName(String primary, String instance, String realm) {
        this.primary = Objects.requireNonNull(primary, "primary must not be null");
        this.instance = instance;
        this.realm = realm;
    }

    public PrincipalName(String primary) {
        this.primary = Objects.requireNonNull(primary, "primary must not be null");
        this.instance = null;
        this.realm = null;
    }

    public String getPrimary() {
        return primary;
    }

    public String getInstance() {
        return instance;
    }

    public String getRealm() {
        return realm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrincipalName that = (PrincipalName) o;
        return primary.equals(that.primary) && Objects.equals(instance, that.instance)
                && Objects.equals(realm, that.realm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primary, instance, realm);
    }

    @Override
    public String toString() {
        return "PrincipalName{" +
                "primary='" + primary + '\'' +
                ", instance='" + instance + '\'' +
                ", realm='" + realm + '\'' +
                '}';
    }
}
