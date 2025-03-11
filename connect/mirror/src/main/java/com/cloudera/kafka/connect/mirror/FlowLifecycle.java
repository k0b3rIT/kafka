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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.mirror.MirrorMaker;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;

import com.cloudera.kafka.connect.mirror.utils.RetryStatus;
import com.cloudera.kafka.connect.mirror.utils.RingBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class FlowLifecycle {
    private static final Logger log = LoggerFactory.getLogger(FlowLifecycle.class);
    private static final int HISTORY_SIZE = 5;

    private final SourceAndTarget flow;
    private final MirrorMaker.MirrorMakerStarter mm;
    private final MirrorMakerMetrics metrics;
    private final CountDownLatch startLatch;
    private final CountDownLatch stopLatch;
    private final int maxTries;
    private final long delayMs;
    private final boolean legacyRestServerModeEnabled;
    private final RingBuffer<RetryStatus> herderStatus;
    private final ExecutorService executor;
    private Herder herder;
    private boolean stopped = false;
    private boolean stopLatchInvoked = false;
    private ConnectRestServer restServer;

    public FlowLifecycle(SourceAndTarget flow, MirrorMaker.MirrorMakerStarter mm, MirrorMakerMetrics metrics,
                         CountDownLatch startLatch, CountDownLatch stopLatch, int maxTries, long delayMs, boolean legacyRestServerModeEnabled) {
        this.flow = flow;
        this.mm = mm;
        this.metrics = metrics;
        this.startLatch = startLatch;
        this.stopLatch = stopLatch;
        this.maxTries = maxTries;
        this.delayMs = delayMs;
        this.legacyRestServerModeEnabled = legacyRestServerModeEnabled;
        herderStatus = RingBuffer.of(new RetryStatus[HISTORY_SIZE], RetryStatus.UNKNOWN);
        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "mm-lifecycle-start-" + flow));
    }

    public synchronized void start(KafkaFuture.BiConsumer<SourceAndTarget, Herder> onFlowStarted) {
        metrics.herderStatus(flow.toString(), herderStatus::getLast);
        IntStream.range(0, HISTORY_SIZE)
                .forEach(i -> metrics.herderStatus(flow.toString(), i, () -> herderStatus.get(i)));

        executor.execute(() -> this.startWithRetries(onFlowStarted));

        startLatch.countDown();
    }

    public synchronized void stop() {
        if (stopped) {
            return;
        }
        new Thread(this::stopInternal, "mm-lifecycle-stop-" + flow).start();
        stopped = true;
    }

    private synchronized void startWithRetries(KafkaFuture.BiConsumer<SourceAndTarget, Herder> onFlowStarted) {
        int attempt = 0;
        do {
            log.info("Start attempt {} out of {}: {}", attempt, maxTries, flow);
            if (attempt != 0 && !trySleeping()) {
                // Thread was interrupted, stop retrying
                return;
            }

            try {
                log.info("Starting the flow: {}", flow);
                herderStatus.add(RetryStatus.STARTING);
                tryStartFlow();
                log.info("Flow started successfully: {}", flow);
                herderStatus.add(RetryStatus.STARTED);
                onFlowStarted.accept(flow, herder);
                return;
            } catch (InterruptedException e) {
                stopDueToInterrupt();
                return;
            } catch (Exception e) {
                herderStatus.add(RetryStatus.FAILED);
                log.warn("Start attempt failed with error: {}", flow, e);
            }
            ++attempt;
        } while (attempt < maxTries || maxTries == -1);

        log.error("All start attempts failed, exiting: {}", flow);
        countDownStopLatch();
        herderStatus.add(RetryStatus.FAILED);
        Exit.exit(1, String.format("All starts attempts of flow %s failed", flow));
    }

    private void stopInternal() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        closeComponentsQuietly();
        metrics.removeHerderUrl(flow.toString());
        metrics.removeHerderMetrics(flow.toString());
        countDownStopLatch();
    }

    private void stopDueToInterrupt() {
        log.info("Interrupted during a start retry attempt: {}", flow);
        herderStatus.add(RetryStatus.INTERRUPTED);
        stop();
    }

    private boolean trySleeping() {
        try {
            log.info("Backing off before attempt: {}", flow);
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            stopDueToInterrupt();
            return false;
        }
        return true;
    }

    private synchronized void tryStartFlow() throws InterruptedException {
        if (stopped) {
            throw new InterruptedException("Flow " + flow + " already stopped");
        }

        try {
            if (legacyRestServerModeEnabled) {
                restServer = mm.createRestServer(flow);
            }
            herder = mm.createHerder(flow, legacyRestServerModeEnabled ? restServer.advertisedUrl() : null);

            herder.start();

            if (legacyRestServerModeEnabled) {
                restServer.initializeResources(herder);

                //herder URL metrics
                metrics.herderUrl(flow.toString(), restServer.advertisedUrl().toString());
            }
        } catch (Exception e) {
            metrics.removeHerderUrl(flow.toString());
            closeComponentsQuietly();
            log.warn("Failed to start flow: {}", flow);
            throw e;
        }
    }

    private synchronized void countDownStopLatch() {
        if (stopLatchInvoked) {
            return;
        }
        stopLatchInvoked = true;
        stopLatch.countDown();
    }

    private void closeComponentsQuietly() {
        if (legacyRestServerModeEnabled && restServer != null) {
            Utils.closeQuietly(restServer::stop, "RestServer of flow " + flow);
            restServer = null;
        }
        if (herder != null) {
            Utils.closeQuietly(herder::stop, "Herder of flow " + flow);
            herder = null;
        }

    }
}
