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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConnectSecretStorageConfigTest {
    @Test
    public void testConfigProcessing() {
        String envVarName = "ENV_VAR_NAME";
        String envVarValue = "value_from_env_var";

        Function<String, String> mockEnvSource = mock(Function.class);
        expect(mockEnvSource.apply(envVarName)).andReturn(envVarValue);
        replay(mockEnvSource);

        Map<String, Object> inputProps = new HashMap<>();
        inputProps.put("some.key", "some_value");
        inputProps.put("some.other.key" + ConnectSecretStorageConfig.ENV_SOURCE_POSTFIX, envVarName);

        Map<String, Object> expectedProps = new HashMap<>();
        expectedProps.put("some.key", "some_value");
        expectedProps.put("some.other.key", envVarValue);

        Map<String, ?> result = ConnectSecretStorageConfig.processProps(inputProps, mockEnvSource);
        assertEquals(expectedProps, result);

        verify(mockEnvSource);
    }
}
