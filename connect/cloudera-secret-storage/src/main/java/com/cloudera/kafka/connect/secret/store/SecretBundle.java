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

/**
 * Represents one bundle (version) of the secrets of a Connector.
 * @param <T> The content type of the bundle.
 */
public class SecretBundle<T> {
    private final String connector;
    private final String id;
    private final long offset;
    private final long timestamp;
    private final T secrets;
    private boolean completed;
    private long completedAtOffset;
    private boolean deleted;
    private long deletedAtOffset;

    public SecretBundle(String connector, String id, long offset, long timestamp, T secrets) {
        this.connector = connector;
        this.id = id;
        this.offset = offset;
        this.timestamp = timestamp;
        this.secrets = secrets;
        completed = false;
        completedAtOffset = -1;
        deleted = false;
        deletedAtOffset = -1;
    }

    public String getConnector() {
        return connector;
    }

    public String getId() {
        return id;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public T getSecrets() {
        return secrets;
    }

    public boolean isCompleted() {
        return completed;
    }

    public long getCompletedAtOffset() {
        return completedAtOffset;
    }

    public void markCompleted(long offset) {
        completed = true;
        completedAtOffset = offset;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public long getDeletedAtOffset() {
        return deletedAtOffset;
    }

    public void markDeleted(long offset) {
        deleted = true;
        deletedAtOffset = offset;
    }

    /**
     * Checks whether this bundle should be considered finalized given a specific deadline and offset.
     * The bundle is considered finalized if:
     * - its creation timestamp is older than the specified deadline
     * - it was marked for completion by an earlier record than the specified offset
     * - it was marked for deletion by an earlier record than the specified offset.
     * @param offsetToCheck The offset to check in case the bundle is marked already.
     * @param deadline The deadline to check the creation time against if the bundle is not marked.
     * @return True if the bundle is considered finalized.
     */
    public boolean isFinalized(long deadline, long offsetToCheck) {
        if (timestamp < deadline) {
            return true;
        }
        if (!completed && !deleted) {
            return false;
        }
        long offsetToCompare = Math.min(
                completedAtOffset == -1 ? Long.MAX_VALUE : completedAtOffset,
                deletedAtOffset == -1 ? Long.MAX_VALUE : deletedAtOffset
        );
        return offsetToCompare < offsetToCheck;
    }
}
