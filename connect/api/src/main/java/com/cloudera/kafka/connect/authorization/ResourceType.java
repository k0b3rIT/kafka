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
// Copyright (c) 2021 Cloudera, Inc. All rights reserved.

package com.cloudera.kafka.connect.authorization;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum ResourceType {
    /** Connector operations:
     *  VIEW - View the Connector and the Task configurations and also the status of its Tasks.
     *  MANAGE - Start/Stop/Pause/Resume etc. Connectors.
     *  EDIT - Edit the config of a Connector.
     *  CREATE - Create a Connector.
     *  DELETE - Delete a Connector.
     */
    CONNECTOR(Operation.VIEW, Operation.MANAGE, Operation.EDIT, Operation.CREATE, Operation.DELETE),

    /** Cluster operations:
     * VIEW - View ConnectorPlugins and the root path.
     * VALIDATE - Validate ConnectorPlugin configuration.
     * MANAGE - Manage cluster resources like loggers.
     */
    CLUSTER(Operation.VIEW, Operation.VALIDATE, Operation.MANAGE);

    private final Set<Operation> operations;

    ResourceType(Operation... operations) {
        this.operations = Arrays.stream(operations)
                .collect(Collectors.toSet());
    }

    public Set<Operation> getOperations() {
        return operations;
    }
}
