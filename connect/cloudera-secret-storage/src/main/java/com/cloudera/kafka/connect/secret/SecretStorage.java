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

import org.apache.kafka.common.Configurable;

import java.util.ConcurrentModificationException;
import java.util.Map;

/**
 * Responsible for storing secret bundles of Connectors in a storage.
 * The stored bundles have IDs, and IDs have a strict order.
 * Implementations must support detecting concurrent, unfinished modifications on the same connector to allow
 * keeping the state of the Connect cluster consistent with the stored secrets.
 */
public interface SecretStorage extends Configurable, AutoCloseable {
    /**
     * Saves a secret bundle into the storage. Provides an id for the saved bundle which can be used with other methods.
     * @param connector The connector to save the secrets for.
     * @param secrets The key-value pairs of secrets to be saved.
     * @return The id of the saved secrets.
     * @throws ConcurrentModificationException When the connector is already being edited in the same time window.
     */
    String saveSecrets(String connector, Map<String, String> secrets) throws ConcurrentModificationException;

    /**
     * Reads all secret properties from a secret bundle for the given connector.
     * @param connector The connector to read secrets for.
     * @param id The id of the secrets to read.
     * @param includeLingeringDeleted If true, returns a bundle even if it was recently deleted, but still available.
     * @return Secrets as key-value pairs or null if the bundle cannot be found.
     */
    Map<String, String> getSecrets(String connector, String id, boolean includeLingeringDeleted);

    /**
     * Deletes a specific bundle of secrets from the storage.
     * @param connector The connector to delete secrets for.
     * @param id The id of secrets to be deleted.
     */
    void deleteSecrets(String connector, String id);

    /**
     * Deletes all previous bundles of the same connector from the storage.
     * @param connector The connector to delete secrets for.
     * @param id The id of secrets to be deleted.
     */
    void deletePreviousSecrets(String connector, String id);

    /**
     * Deletes a bundle of secrets and all previous bundles of the same connector from the storage.
     * @param connector The connector to delete secrets for.
     * @param id The id of secrets to be deleted.
     */
    void deleteSecretsAndPreviousSecrets(String connector, String id);

    /**
     * Marks the specified bundle as completed, and cleans up previous bundles for the same connector.
     * @param connector The connector to manage secrets for.
     * @param id The id to mark completed and clean up until.
     */
    void markForCompletionAndDeletePrevious(String connector, String id);
}
