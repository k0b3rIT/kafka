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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MultiLevelRack {
    private List<String> rackNames;

    public MultiLevelRack(List<String> rackNames) {
        this.rackNames = rackNames;
    }

    public MultiLevelRack() {
        this.rackNames = new ArrayList<>();
    }

    public List<String> rackNames() {
        return rackNames;
    }

    public int size() {
        return rackNames.size();
    }

    public MultiLevelRack take(int level) {
        if (level > rackNames.size()) {
            return new MultiLevelRack(rackNames);
        }
        return new MultiLevelRack(rackNames.subList(0, level));
    }

    public boolean startsWith(MultiLevelRack rack) {
        if (rack.size() > this.size()) {
            return false;
        }
        return rack.rackNames().equals(this.rackNames.subList(0, rack.size()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiLevelRack that = (MultiLevelRack) o;
        return Objects.equals(rackNames, that.rackNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rackNames);
    }
}
