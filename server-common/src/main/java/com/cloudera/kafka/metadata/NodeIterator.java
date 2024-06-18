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

import java.util.List;

public class NodeIterator<T> extends TreeIterator<T> {
    private final MultiLevelRack rack;
    private final List<TreeIterator<T>> iterators;
    private int currentIndex = -1;

    public NodeIterator(MultiLevelRack rack,  List<TreeIterator<T>> iterators) {
        this.rack = rack;
        this.iterators = iterators;
    }

    @Override
    public MultiLevelRack rack() {
        return rack;
    }

    @Override
    public int cycle() {
        return iterators.size() * iterators.stream().mapToInt(TreeIterator::cycle).reduce(1, (a, b) -> a * b);
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
        return iterators.get(++currentIndex % iterators.size()).next();
    }
}
