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
package com.cloudera.kafka.common.replica;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.replica.ClientMetadata;
import org.apache.kafka.common.replica.PartitionView;
import org.apache.kafka.common.replica.ReplicaSelector;
import org.apache.kafka.common.replica.ReplicaView;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MultiLevelRackAwareReplicaSelector implements ReplicaSelector {

    @Override
    public Optional<ReplicaView> select(TopicPartition topicPartition,
                                        ClientMetadata clientMetadata,
                                        PartitionView partitionView) {
        String clientRackId = clientMetadata.rackId();
        if (validRackId(clientRackId))  {
            List<ReplicaView> result = longestMatch(excludeInvalidReplicas(partitionView.replicas()),
                clientRackId,
                x -> x.endpoint().rack());
            if (result.isEmpty()) {
                Optional.of(partitionView.leader());
            } else if (result.size() == 1) {
                return Optional.of(result.get(0));
            } else {
                return Optional.of(result.get(ThreadLocalRandom.current().nextInt(result.size())));
            }
        }
        return Optional.of(partitionView.leader());
    }

    static <T> List<T> longestMatch(Set<T> itemsToFilter, String stringToMatch, Function<T, String> stringExtractor) {
        if (stringToMatch == null) {
            return new ArrayList<>(itemsToFilter);
        }
        Set<T> matchingSet = new HashSet<>(itemsToFilter);
        Set<T> previousMatchingSet = new HashSet<>();
        int from = 0;
        while (from < stringToMatch.length()) {
            int to = stringToMatch.indexOf("/", from + 1);
            if (to < 0) {
                to = stringToMatch.length();
            }
            Iterator<T> it = matchingSet.iterator();
            while (it.hasNext()) {
                String rack = stringExtractor.apply(it.next());
                if (!stringToMatch.regionMatches(from, rack, from, to - from)) {
                    it.remove();
                }
            }
            if (matchingSet.isEmpty()) {
                break;
            }
            previousMatchingSet = new HashSet<>(matchingSet);
            from = to;
        }
        List<T> result = matchingSet.isEmpty() ? new ArrayList<>(previousMatchingSet) : new ArrayList<>(matchingSet);
        return result;
    }

    private static Set<ReplicaView> excludeInvalidReplicas(Set<ReplicaView> matchingReplicas) {
        return matchingReplicas.stream()
            .filter(x -> validRackId(x.endpoint().rack()))
            .collect(Collectors.toSet());
    }

    private static boolean validRackId(String clientRackId) {
        return clientRackId != null &&
            !clientRackId.isEmpty() &&
            clientRackId.charAt(0) == '/';
    }
}
