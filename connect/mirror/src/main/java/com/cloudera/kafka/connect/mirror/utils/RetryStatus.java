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
package com.cloudera.kafka.connect.mirror.utils;

public enum RetryStatus {
    STARTED(0, false, true),

    UNKNOWN(1, true, false),
    STARTING(2, false, false),
    INTERRUPTED(3, false, false),
    FAILED(4, true, false);

    private final int statusCode;
    private final boolean retriable;
    private final boolean success;

    RetryStatus(int statusCode, boolean retriable, boolean success) {
        this.statusCode = statusCode;
        this.retriable = retriable;
        this.success = success;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public boolean isRetriable() {
        return retriable;
    }

    public boolean isSuccess() {
        return success;
    }
}
