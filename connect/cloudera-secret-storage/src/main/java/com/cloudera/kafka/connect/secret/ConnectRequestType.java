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

import com.cloudera.kafka.connect.common.ConnectRestFilterUtils.MethodType;

import java.util.Arrays;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

import static com.cloudera.kafka.connect.common.ConnectRestFilterUtils.CREATE_PATH_PATTERN;
import static com.cloudera.kafka.connect.common.ConnectRestFilterUtils.DELETE_PATH_PATTERN;
import static com.cloudera.kafka.connect.common.ConnectRestFilterUtils.EDIT_PATH_PATTERN;

public enum ConnectRequestType {
    CREATE(
            new MethodType(HttpMethod.POST, CREATE_PATH_PATTERN),
            Response.Status.CREATED.getStatusCode()),
    EDIT(
            new MethodType(HttpMethod.PUT, EDIT_PATH_PATTERN),
            Response.Status.OK.getStatusCode(), Response.Status.CREATED.getStatusCode()),
    DELETE(
            new MethodType(HttpMethod.DELETE, DELETE_PATH_PATTERN),
            Response.Status.NO_CONTENT.getStatusCode()),
    OTHER(null);

    private final MethodType methodType;
    private final int[] successStatuses;

    ConnectRequestType(MethodType methodType, int... successStatuses) {
        this.methodType = methodType;
        this.successStatuses = successStatuses;
    }

    public static boolean requestFailed(ConnectRequestType requestType, int statusCode) {
        return Arrays.stream(requestType.successStatuses).noneMatch(successStatus -> successStatus == statusCode);
    }

    public boolean matchesRequestType(String requestMethod, String requestPath) {
        return this.methodType.matchingWithMethodAndPath(requestMethod, requestPath);
    }
}
