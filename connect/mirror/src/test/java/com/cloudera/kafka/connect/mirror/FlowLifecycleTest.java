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
package com.cloudera.kafka.connect.mirror;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.mirror.MirrorMaker;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.Herder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FlowLifecycleTest {
    private static final String SOURCE_ALIAS = "source";
    private static final String TARGET_ALIAS = "target";
    private static final SourceAndTarget SOURCE_AND_TARGET = new SourceAndTarget(SOURCE_ALIAS, TARGET_ALIAS);

    private static final long RETRY_DELAY_MS = 10;
    private static final long VERIFY_TIMEOUT_MS = 2000;


    @Mock
    Herder mockHerder;
    @Mock
    MirrorMaker.MirrorMakerStarter mockStarter;
    @Mock
    MirrorMakerMetrics mockMetrics;
    @Mock
    CountDownLatch mockStartLatch;
    @Mock
    CountDownLatch mockStopLatch;
    @Mock
    Exit.Procedure mockExitCallback;
    FlowLifecycle lifecycle;
    boolean checkExitNotCalled;

    @BeforeEach
    public void setup() {
        checkExitNotCalled = true;
        Exit.setExitProcedure(mockExitCallback);
        when(mockStarter.createHerder(any())).thenReturn(mockHerder);
    }

    @AfterEach
    public void stop() {
        if (lifecycle != null) {
            lifecycle.stop();
        }
        verify(mockStopLatch, timeout(30 * 1000)).countDown();
        if (checkExitNotCalled) {
            verify(mockExitCallback, never()).execute(anyInt(), anyString());
        }
        Exit.resetExitProcedure();
    }

    @Test
    public void testSuccessfulFirstTry() {
        createLifecycle(1);

        lifecycle.start((a, b) -> { });

        verifyStartSync();
        verifyTimeout(mockStarter).createHerder(eq(SOURCE_AND_TARGET));
        verifyTimeout(mockHerder).start();

        lifecycle.stop();

        verifyTimeout(mockHerder).stop();
        verifyTimeout(mockMetrics).removeHerderUrl(SOURCE_AND_TARGET.toString());
        verifyTimeout(mockMetrics).removeHerderMetrics(SOURCE_AND_TARGET.toString());
        verifyTimeout(mockStopLatch).countDown();
        verifyTimeoutAndTimes(mockExitCallback, 0).execute(anyInt(), anyString());
    }


    @Test
    public void testCreateHerderFailsFirstTryThenSucceeds() {
        createLifecycle(2);

        when(mockStarter.createHerder(any()))
                .thenThrow(new RuntimeException("Test herder failure"))
                .thenReturn(mockHerder);

        lifecycle.start((a, b) -> { });

        verifyStartSync();
        verifyTimeout(mockMetrics).removeHerderUrl(SOURCE_AND_TARGET.toString());

        verifyTimeoutAndTimes(mockStarter, 2).createHerder(eq(SOURCE_AND_TARGET));
        verifyTimeout(mockHerder).start();
        verifyTimeoutAndTimes(mockExitCallback, 0).execute(anyInt(), anyString());
    }

    private void verifyStartSync() {
        verify(mockMetrics).herderStatus(eq(SOURCE_AND_TARGET.toString()), any());
        verify(mockStartLatch).countDown();
    }

    private <T> T verifyTimeout(T mock) {
        return verify(mock, timeout(VERIFY_TIMEOUT_MS));
    }

    private <T> T verifyTimeoutAndTimes(T mock, int times) {
        return verify(mock, timeout(VERIFY_TIMEOUT_MS).times(times));
    }

    private void createLifecycle(int maxRetries) {
        lifecycle = new FlowLifecycle(SOURCE_AND_TARGET, mockStarter, mockMetrics, mockStartLatch, mockStopLatch,
                maxRetries, RETRY_DELAY_MS);
    }
}
