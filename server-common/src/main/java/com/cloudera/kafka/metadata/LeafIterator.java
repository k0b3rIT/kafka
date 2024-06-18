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
// Copyright (c) 2024 Cloudera, Inc. All rights reserved.
package com.cloudera.kafka.metadata;

import java.util.Iterator;
import java.util.List;

public class LeafIterator<T> extends TreeIterator<T> {
    private final MultiLevelRack rack;
    private final List<T> brokers;
    private Iterator<T> iter;

    public LeafIterator(MultiLevelRack rack, List<T> brokers) {
        this.rack = rack;
        this.brokers = brokers;
        iter = brokers.iterator();
    }

    @Override
    public MultiLevelRack rack() {
        return rack;
    }

    @Override
    public int cycle() {
        return brokers.size();
    }

    @Override
    public void advance(int index) {
        while (index > 0) {
            this.next();
            index--;
        }
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public T next() {
        if (!iter.hasNext()) {
            iter = brokers.iterator();
        }
        return iter.next();
    }
}
