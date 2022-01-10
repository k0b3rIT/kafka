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
package com.cloudera.kafka.connect.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.regex.Pattern;

public class ConnectRestFilterUtils {

    public static final Pattern CREATE_PATH_PATTERN = java.util.regex.Pattern.compile("^connectors[/]?");
    public static final Pattern EDIT_PATH_PATTERN = java.util.regex.Pattern.compile("^connectors/[^/]+/config[/]?");
    public static final Pattern DELETE_PATH_PATTERN = java.util.regex.Pattern.compile("^connectors/[^/]+[/]?");
    private static final Logger log = LoggerFactory.getLogger(ConnectRestFilterUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static byte[] inputStreamToByteArray(InputStream inputStream) {
        byte[] buffer = new byte[1024];
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            int readLen;
            while ((readLen = inputStream.read(buffer)) != -1) {
                bos.write(buffer, 0, readLen);
            }
            return bos.toByteArray();
        } catch (IOException e) {
            log.error("Error reading request body", e);
            return null;
        }
    }

    public static String extractConnectorNameFromPath(String requestUriPath) {
        String subPath = requestUriPath.replaceFirst("connectors/", "");
        int position = subPath.indexOf('/');
        return position == -1 ? subPath : subPath.substring(0, position);
    }

    public static class MethodType {
        private final String httpMethod;
        private final Pattern pathPattern;

        public MethodType(String httpMethod, Pattern pathPattern) {
            this.httpMethod = httpMethod;
            this.pathPattern = pathPattern;
        }

        public Pattern getPathPattern() {
            return pathPattern;
        }

        public boolean matchingWithMethodAndPath(String httpMethod, String path) {
            return this.httpMethod.equals(httpMethod) && pathPattern.matcher(path).matches();
        }
    }

    /**
     * Same structure as org.apache.kafka.connect.runtime.rest.entities.ErrorMessage.
     * org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper produces error responses
     * using this structure, the filter should also use it in order for Connect Workers to be able to
     * deserialize responses.
     */
    public static class ErrorMessage {
        private final int errorCode;
        private final String message;

        @JsonCreator
        public ErrorMessage(
                @JsonProperty("error_code") int errorCode, @JsonProperty("message") String message) {
            this.errorCode = errorCode;
            this.message = message;
        }

        @JsonProperty("error_code")
        public int errorCode() {
            return errorCode;
        }

        @JsonProperty
        public String message() {
            return message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ErrorMessage that = (ErrorMessage) o;
            return Objects.equals(errorCode, that.errorCode) && Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(errorCode, message);
        }
    }
}
