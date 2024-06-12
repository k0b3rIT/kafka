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

import java.util.Arrays;

public class RingBuffer<T> {
    private final T[] backingArray;
    private final int capacity;
    private int idx = 0;

    public static <R> RingBuffer<R> of(R[] backing, R neutral) {
        return new RingBuffer<>(backing, neutral);
    }

    private RingBuffer(T[] backing, T neutral) {
        this.backingArray = backing;
        this.capacity = backing.length;
        Arrays.fill(backing, neutral);
    }

    public synchronized void add(T t) {
        backingArray[idx] = t;
        idx = (idx + 1) % capacity;
    }

    public synchronized T get(int i) {
        return backingArray[(idx + i) % capacity];
    }

    public T getLast() {
        return get(capacity - 1);
    }
}
