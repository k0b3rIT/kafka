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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.replica.ClientMetadata;
import org.apache.kafka.common.replica.PartitionView;
import org.apache.kafka.common.replica.ReplicaSelector;
import org.apache.kafka.common.replica.ReplicaView;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

class MultiLevelRackAwareReplicaSelectorTest {

    @ParameterizedTest
    @ValueSource(strings = {"/DC1/R1/VM0", "/DC2/R0/VM0"})
    public void itShouldReturnReplicaWithSameRackId(String consumerRack) {
        TopicPartition tp = new TopicPartition("test", 0);
        List<ReplicaView> replicaViewSet = getReplicaViews();

        ReplicaView leader = replicaViewSet.get(0);
        PartitionView partitionView = partitionInfo(new HashSet<>(replicaViewSet), leader);

        ReplicaSelector selector = new MultiLevelRackAwareReplicaSelector();
        Optional<ReplicaView> selected = selector.select(tp, metadata(consumerRack), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertThat(consumerRack, equalTo(replicaInfo.endpoint().rack()));
        });
    }

    @ParameterizedTest
    @CsvSource({
        "/DC1/R1/VM3, /DC1/R1",
        "/DC2/R1/VM3, /DC2/R1"
    })
    public void itShouldReturnReplicaWithClosestPrefix(String consumerRack, String prefix) {
        TopicPartition tp = new TopicPartition("test", 0);
        List<ReplicaView> replicaViewSet = getReplicaViews();

        ReplicaView leader = replicaViewSet.get(0);
        PartitionView partitionView = partitionInfo(new HashSet<>(replicaViewSet), leader);

        ReplicaSelector selector = new MultiLevelRackAwareReplicaSelector();
        Optional<ReplicaView> selected = selector.select(tp, metadata(consumerRack), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertThat(consumerRack, not(equalTo(replicaInfo.endpoint().rack())));
            assertThat(replicaInfo.endpoint().rack(), startsWith(prefix));
        });
    }

    @Test
    public void itShouldReturnLeaderWhenConsumerDoesNotMatchAny() {
        TopicPartition tp = new TopicPartition("test", 0);
        List<ReplicaView> replicaViewSet = getReplicaViews();

        ReplicaView leader = replicaViewSet.get(0);
        PartitionView partitionView = partitionInfo(new HashSet<>(replicaViewSet), leader);

        ReplicaSelector selector = new MultiLevelRackAwareReplicaSelector();
        Optional<ReplicaView> selected = selector.select(tp, metadata("/DC3/R1"), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertThat(replicaInfo.endpoint().rack(), not(startsWith("/DC3")));
            assertThat(leader.endpoint().rack(), equalTo(replicaInfo.endpoint().rack()));
            assertThat(leader.endpoint().id(), equalTo(0));
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"/DC1", "/DC2"})
    public void itShouldReturnReplicaWithMatchingPrefix(String consumerRack) {
        TopicPartition tp = new TopicPartition("test", 0);
        List<ReplicaView> replicaViewSet = getReplicaViews();

        ReplicaView leader = replicaViewSet.get(0);
        PartitionView partitionView = partitionInfo(new HashSet<>(replicaViewSet), leader);
        ReplicaSelector selector = new MultiLevelRackAwareReplicaSelector();
        Optional<ReplicaView> selected = selector.select(tp, metadata(consumerRack), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertThat(replicaInfo.endpoint().rack(), startsWith(consumerRack));
        });
    }

    @Test
    public void itShouldReturnLeaderWhenConsumerPrefixDoesNotMatchAny() {
        TopicPartition tp = new TopicPartition("test", 0);
        List<ReplicaView> replicaViewSet = getReplicaViews();

        ReplicaView leader = replicaViewSet.get(0);
        PartitionView partitionView = partitionInfo(new HashSet<>(replicaViewSet), leader);

        ReplicaSelector selector = new MultiLevelRackAwareReplicaSelector();
        Optional<ReplicaView> selected = selector.select(tp, metadata("/DC3"), partitionView);
        assertOptional(selected, replicaInfo -> {
            assertThat(replicaInfo.endpoint().rack(), not(startsWith("/DC3")));
            assertThat(leader.endpoint().rack(), equalTo(replicaInfo.endpoint().rack()));
            assertThat(leader.endpoint().id(), equalTo(0));
        });
    }

    @Test
    public void testLongestMatch() {
        Set<String> input = elements("/DC1/R0/VM0", "/DC1/R1/VM0", "/DC2/R3/VM0");
        Function<String, String> f = x -> x;
        assertThat(MultiLevelRackAwareReplicaSelector.longestMatch(input, null, f),
            containsInAnyOrder("/DC1/R0/VM0", "/DC1/R1/VM0", "/DC2/R3/VM0"));

        assertThat(MultiLevelRackAwareReplicaSelector.longestMatch(input, "", f),
            containsInAnyOrder("/DC1/R0/VM0", "/DC1/R1/VM0", "/DC2/R3/VM0"));

        assertThat(MultiLevelRackAwareReplicaSelector.longestMatch(input, "notmatching", f),
            hasSize(0));

        assertThat(MultiLevelRackAwareReplicaSelector.longestMatch(input, "/", f),
            containsInAnyOrder("/DC1/R0/VM0", "/DC1/R1/VM0", "/DC2/R3/VM0"));

        assertThat(MultiLevelRackAwareReplicaSelector.longestMatch(input, "/DC1/", f),
            containsInAnyOrder("/DC1/R0/VM0", "/DC1/R1/VM0"));

        assertThat(MultiLevelRackAwareReplicaSelector.longestMatch(input, "/DC1/R1", f),
            Matchers.contains("/DC1/R1/VM0"));

        assertThat(MultiLevelRackAwareReplicaSelector.longestMatch(input, "/DC1/R1/VM0", f),
            Matchers.contains("/DC1/R1/VM0"));

        assertThat(MultiLevelRackAwareReplicaSelector.longestMatch(input, "/DC1/R1/VM0/none", f),
            Matchers.contains("/DC1/R1/VM0"));
    }

    private List<ReplicaView> getReplicaViews() {
        return Stream.of(
            replicaInfo(new Node(0, "host0", 1234, "/DC1/R0/VM0"), 4, 0),
            replicaInfo(new Node(1, "host0", 1234, "/DC1/R0/VM1"), 4, 0),
            replicaInfo(new Node(2, "host1", 1234, "/DC1/R1/VM0"), 2, 5),
            replicaInfo(new Node(3, "host1", 1234, "/DC1/R1/VM1"), 2, 5),
            replicaInfo(new Node(4, "host2", 1234, "/DC2/R0/VM0"), 3, 3),
            replicaInfo(new Node(5, "host2", 1234, "/DC2/R0/VM1"), 3, 3),
            replicaInfo(new Node(6, "host3", 1234, "/DC2/R1/VM0"), 4, 2),
            replicaInfo(new Node(7, "host3", 1234, "/DC2/R1/VM1"), 4, 2)
        ).collect(Collectors.toList());
    }
    private Set<String> elements(String... elements) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(elements)));
    }

    static ReplicaView replicaInfo(Node node, long logOffset, long timeSinceLastCaughtUpMs) {
        return new ReplicaView.DefaultReplicaView(node, logOffset, timeSinceLastCaughtUpMs);
    }

    static PartitionView partitionInfo(Set<ReplicaView> replicaViewSet, ReplicaView leader) {
        return new PartitionView.DefaultPartitionView(replicaViewSet, leader);
    }

    static ClientMetadata metadata(String rack) {
        return new ClientMetadata.DefaultClientMetadata(rack, "test-client",
            InetAddress.getLoopbackAddress(), KafkaPrincipal.ANONYMOUS, "TEST");
    }
}