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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class RingBufferTest {

    private final int capacity = 100;

    private Random rnd;
    private RingBuffer<RetryStatus> buffer;

    @BeforeEach
    public void setUp() {
        rnd = new Random(42L);
        buffer = RingBuffer.of(new RetryStatus[capacity], RetryStatus.UNKNOWN);
    }


    /**
     * Basic write and read
     */
    @Test
    public void testSimpleReadWrite() {
        final int upto = 30;
        IntStream.range(0, upto).forEach(i -> buffer.add(RetryStatus.STARTING));

        // neutral (not inserted by any add) elements are at the lower indices
        // inserted elements are after them
        List<RetryStatus> expected = prepareSample(RetryStatus.UNKNOWN, capacity - upto, RetryStatus.STARTING, upto);

        IntStream.range(0, capacity).forEach(i -> {
            assertEquals(expected.get(i), buffer.get(i), "Buffer value is incorrectly set at index");
        });
    }

    /**
     * Fill over its capacity, older elements are forgotten
     */
    @Test
    public void testOverrunReadAndWrite() {
        List<RetryStatus> sample = prepareRandomSample(2 * capacity);
        sample.forEach(buffer::add);
        List<RetryStatus> lastElements = sample.subList(sample.size() - capacity, sample.size());

        // older elements are "forgotten"
        IntStream.range(0, capacity).forEach(i -> {
            assertEquals(lastElements.get(i), buffer.get(i), "Buffer value is incorrectly set at index");
        });

    }

    private List<RetryStatus> prepareSample(RetryStatus status0, int count0, RetryStatus status1, int count1) {
        List<RetryStatus> sample = new ArrayList<>();

        int i = 0;
        for (int j = 0; j < count0; ++j, ++i) {
            sample.add(status0);
        }
        for (int j = 0; j < count1; ++j, ++i) {
            sample.add(status1);
        }

        return sample;
    }

    private List<RetryStatus> prepareRandomSample(int totalStatuses) {
        List<RetryStatus> sample = new ArrayList<>();

        IntStream.range(0, totalStatuses).forEach(i -> {
            int randomOrdinal =  rnd.nextInt(RetryStatus.values().length);
            sample.add(RetryStatus.values()[randomOrdinal]);
        });

        return sample;
    }


}