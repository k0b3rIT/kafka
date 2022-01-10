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

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.net.MalformedURLException;

public class TestUtils {

    public static HttpGet createHttpGetRequest(String serverBaseUrl, String path) throws MalformedURLException {
        return new HttpGet(createUrl(serverBaseUrl, path));
    }

    public static HttpPut createHttpPutRequest(String serverBaseUrl, String path) throws MalformedURLException {
        HttpPut request = new HttpPut(createUrl(serverBaseUrl, path));
        setRequestEntity(request, "{}");
        return request;
    }

    public static HttpPut createHttpPutRequest(String serverBaseUrl, String path, String json) throws MalformedURLException {
        HttpPut request = new HttpPut(createUrl(serverBaseUrl, path));
        setRequestEntity(request, json);
        return request;
    }

    public static HttpDelete createHttpDeleteRequest(String serverBaseUrl, String path) throws MalformedURLException {
        return new HttpDelete(createUrl(serverBaseUrl, path));
    }

    public static HttpPost createHttpPostRequest(String serverBaseUrl, String path, String json) throws MalformedURLException {
        HttpPost request = new HttpPost(createUrl(serverBaseUrl, path));
        setRequestEntity(request, json);
        return request;
    }

    public static String createUrl(String serverBaseUrl, String path) throws MalformedURLException {
        StringBuilder urlBuilder = new StringBuilder(serverBaseUrl);
        return urlBuilder.append(path).toString();
    }

    public static void setRequestEntity(HttpEntityEnclosingRequestBase request, String json) {
        StringEntity entity = new StringEntity(json, "UTF-8");
        entity.setContentType("application/json");
        request.setEntity(entity);
    }

    public static HttpResponse execute(HttpRequestBase request) throws IOException {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            return client.execute(request);
        }
    }
}
