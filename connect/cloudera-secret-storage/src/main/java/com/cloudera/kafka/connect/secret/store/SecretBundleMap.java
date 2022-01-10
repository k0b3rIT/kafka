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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Responsible for storing multiple versions of connector secrets.
 * Keeps track of ID -> version mappings to perform reverse lookups.
 * @param <T> The content type of the stored secret bundles.
 */
public class SecretBundleMap<T> {
    private final Map<String, LinkedHashMap<String, SecretBundle<T>>> bundles;

    public SecretBundleMap() {
        bundles = new HashMap<>();
    }

    private SecretBundleMap(Map<String, LinkedHashMap<String, SecretBundle<T>>> bundles) {
        this.bundles = bundles;
    }

    public SecretBundle<T> getBundle(String connector, String id) {
        Map<String, SecretBundle<T>> connectorBundles = bundles.get(connector);
        if (connectorBundles == null) {
            return null;
        }
        return connectorBundles.get(id);
    }

    public List<SecretBundle<T>> findPreviousBundles(String connector, String id) {
        Map<String, SecretBundle<T>> connectorBundles = bundles.get(connector);
        if (connectorBundles == null) {
            return null;
        }
        List<SecretBundle<T>> olderBundles = new ArrayList<>();
        for (Map.Entry<String, SecretBundle<T>> entry : connectorBundles.entrySet()) {
            if (id.equals(entry.getKey())) {
                break;
            }
            olderBundles.add(entry.getValue());
        }
        return olderBundles;
    }

    public boolean addBundle(SecretBundle<T> bundle) {
        SecretBundle<T> prevBundle = bundles
                .computeIfAbsent(bundle.getConnector(), k -> new LinkedHashMap<>())
                .put(bundle.getId(), bundle);
        return prevBundle != null;
    }

    public boolean removeBundle(String connector, String id) {
        Map<String, SecretBundle<T>> connectorBundles = bundles.get(connector);
        if (connectorBundles == null) {
            return false;
        }
        SecretBundle<T> bundle = connectorBundles.remove(id);
        if (bundle == null) {
            return false;
        }
        if (connectorBundles.isEmpty()) {
            bundles.remove(connector);
        }
        return true;
    }

    public boolean markBundleCompleted(String connector, String id, long offset) {
        Map<String, SecretBundle<T>> connectorBundles = bundles.get(connector);
        if (connectorBundles == null) {
            return false;
        }
        SecretBundle<T> bundle = connectorBundles.get(id);
        if (bundle == null) {
            return false;
        }
        bundle.markCompleted(offset);
        return true;
    }

    public boolean markBundleDeleted(String connector, String id, long offset) {
        Map<String, SecretBundle<T>> connectorBundles = bundles.get(connector);
        if (connectorBundles == null) {
            return false;
        }
        SecretBundle<T> bundle = connectorBundles.get(id);
        if (bundle == null) {
            return false;
        }
        bundle.markDeleted(offset);
        return true;
    }

    public void forEach(BiConsumer<String, LinkedHashMap<String, SecretBundle<T>>> consumer) {
        bundles.forEach(consumer);
    }

    public SecretBundleMap<T> copy() {
        Map<String, LinkedHashMap<String, SecretBundle<T>>> copiedBundles = new HashMap<>();
        forEach((connector, connectorBundles) -> copiedBundles.put(connector, new LinkedHashMap<>(connectorBundles)));
        return new SecretBundleMap<>(copiedBundles);
    }
}
