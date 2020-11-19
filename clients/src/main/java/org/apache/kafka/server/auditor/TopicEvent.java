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

package org.apache.kafka.server.auditor;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;
import java.util.Set;

@InterfaceStability.Unstable
public class TopicEvent implements AuditEvent {

    public enum EventType {
        CREATE, DELETE, PARTITION_CHANGE, REPLICATION_FACTOR_CHANGE;
    }

    public static class AuditedTopic {
        private final String topicName;
        private final int numPartitions;
        private final int replicationFactor;

        private static final int NO_REPLICATION_FACTOR = -1;
        private static final int NO_PARTITION_NUMBER = -1;

        public AuditedTopic(String topicName) {
            this.topicName = topicName;
            this.numPartitions = NO_PARTITION_NUMBER;
            this.replicationFactor = NO_REPLICATION_FACTOR;
        }

        public AuditedTopic(String topicName, int numPartitions, int replicationFactor) {
            this.topicName = topicName;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
        }

        public static AuditedTopic withReplicationFactor(String topicName, int replicationFactor) {
            return new AuditedTopic(topicName, NO_PARTITION_NUMBER, replicationFactor);
        }

        public static AuditedTopic withPartitionNumber(String topicName, int partitionNumber) {
            return new AuditedTopic(topicName, partitionNumber, NO_REPLICATION_FACTOR);
        }

        public String name() {
            return topicName;
        }

        public int numPartitions() {
            return numPartitions;
        }

        public int replicationFactor() {
            return replicationFactor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AuditedTopic that = (AuditedTopic) o;
            return numPartitions == that.numPartitions &&
                replicationFactor == that.replicationFactor &&
                Objects.equals(topicName, that.topicName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicName, numPartitions, replicationFactor);
        }
    }

    private final Set<AuditedTopic> topics;
    private final EventType eventType;

    public TopicEvent(Set<AuditedTopic> topics, EventType eventType) {
        this.topics = topics;
        this.eventType = eventType;
    }

    public Set<AuditedTopic> topics() {
        return topics;
    }

    public EventType eventType() {
        return eventType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicEvent that = (TopicEvent) o;
        return topics.equals(that.topics) &&
            eventType == that.eventType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topics, eventType);
    }
}
