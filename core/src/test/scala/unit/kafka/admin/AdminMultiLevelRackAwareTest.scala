/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.admin.AdminUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AdminMultiLevelRackAwareTest extends RackAwareTest with MultiLevelRackAwareTest with Logging {

  @Test
  def testAssignmentWithRackAware(): Unit = {
    val brokerRackMapping = Map(0 -> "/rack1", 1 -> "/rack2", 2 -> "/rack2", 3 -> "/rack3", 4 -> "/rack3", 5 -> "/rack1")
    val numPartitions = 6
    val replicationFactor = 3
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, 2, 0, true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWithRackAwareWithUnevenReplicas(): Unit = {
    val brokerRackMapping = Map(0 -> "/rack1", 1 -> "/rack2", 2 -> "/rack2", 3 -> "/rack3", 4 -> "/rack3", 5 -> "/rack1")
    val numPartitions = 13
    val replicationFactor = 3
    val assignment =  CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, 0, 0, true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor, verifyLeaderDistribution = false, verifyReplicasDistribution = false)
  }

  @Test
  def testAssignmentWithRackAwareWithUnevenRacks(): Unit = {
    val brokerRackMapping = Map(0 -> "/rack1", 1 -> "/rack1", 2 -> "/rack2", 3 -> "/rack3", 4 -> "/rack3", 5 -> "/rack1")
    val numPartitions = 12
    val replicationFactor = 3
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, 0, 0, true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor, verifyReplicasDistribution = false)
  }

  @Test
  def testRackAwareExpansion(): Unit = {
    val brokerRackMapping = Map(6 -> "/rack1", 7 -> "/rack2", 8 -> "/rack2", 9 -> "/rack3", 10 -> "/rack3", 11 -> "/rack1")
    val numPartitions = 12
    val replicationFactor = 2
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, 12, 0, true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6Partitions(): Unit = {
    val brokerRackMapping = Map(0 -> "/rack1", 1 -> "/rack2", 2 -> "/rack2", 3 -> "/rack3", 4 -> "/rack3", 5 -> "/rack1")
    val numPartitions = 6
    val replicationFactor = 2
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testAssignmentWith2ReplicasRackAwareWith6PartitionsAnd3Brokers(): Unit = {
    val brokerRackMapping = Map(0 -> "/rack1", 1 -> "/rack2", 4 -> "/rack3")
    val numPartitions = 3
    val replicationFactor = 2
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor,
      true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions, replicationFactor)
  }

  @Test
  def testLargeNumberPartitionsAssignment(): Unit = {
    val numPartitions = 96
    val replicationFactor = 3
    val brokerRackMapping = Map(0 -> "/rack1", 1 -> "/rack2", 2 -> "/rack2", 3 -> "/rack3", 4 -> "/rack3", 5 -> "/rack1",
      6 -> "/rack1", 7 -> "/rack2", 8 -> "/rack2", 9 -> "/rack3", 10 -> "/rack1", 11 -> "/rack3")
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
  }

  @Test
  def testMoreReplicasThanRacks(): Unit = {
    val numPartitions = 6
    val replicationFactor = 5
    val brokerRackMapping = Map(0 -> "/rack1", 1 -> "/rack2", 2 -> "/rack2", 3 -> "/rack3", 4 -> "/rack3", 5 -> "/rack2")
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor,
      true))
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.toIndexedSeq.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 until numPartitions)
      assertEquals(3, distribution.partitionRacks(partition).toSet.size)
  }

  @Test
  def testLessReplicasThanRacks(): Unit = {
    val numPartitions = 6
    val replicationFactor = 2
    val brokerRackMapping = Map(0 -> "/rack1", 1 -> "/rack2", 2 -> "/rack2", 3 -> "/rack3", 4 -> "/rack3", 5 -> "/rack2")
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, true))
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.toIndexedSeq.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 to 5)
      assertEquals(2, distribution.partitionRacks(partition).toSet.size)
  }


  @Test
  def testSingleRack(): Unit = {
    val numPartitions = 6
    val replicationFactor = 3
    val brokerRackMapping = Map(0 -> "/rack1", 1 -> "/rack1", 2 -> "/rack1", 3 -> "/rack1", 4 -> "/rack1", 5 -> "/rack1")
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor,
      true))
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.toIndexedSeq.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 until numPartitions)
      assertEquals(1, distribution.partitionRacks(partition).toSet.size)
    for (broker <- brokerRackMapping.keys)
      assertEquals(1, distribution.brokerLeaderCount(broker))
  }

  @Test
  def testSkipBrokerWithReplicaAlreadyAssigned(): Unit = {
    val rackInfo = Map(0 -> "/a", 1 -> "/b", 2 -> "/c", 3 -> "/a", 4 -> "/a")
    val numPartitions = 6
    val replicationFactor = 4
    val brokerMetadatas = toBrokerMetadata(rackInfo)
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(brokerMetadatas, numPartitions, replicationFactor,
      2, 0, true))
    checkReplicaDistribution(assignment, rackInfo, 5, 6, 4,
      verifyRackAware = false, verifyLeaderDistribution = false, verifyReplicasDistribution = false)
  }

  @Test
  def testAssignmentWithMlRackAware(): Unit = {
    val brokerRackMapping = Map(
      0 -> "/DC1/rack1", 1 -> "/DC1/rack1", 2 -> "/DC1/rack2", 3 -> "/DC1/rack2",
      4 -> "/DC2/rack1", 5 -> "/DC2/rack1", 6 -> "/DC2/rack2", 7 -> "/DC2/rack2")
    val numPartitions = 8
    val replicationFactor = 4
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor,
      true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
    checkMultiLevelReplicaDistribution(assignment, brokerRackMapping)
  }

  @Test
  def testAssignmentWithMlRackAwareWithUnevenReplicas(): Unit = {
    val brokerRackMapping = Map(
      0 -> "/DC1/rack1", 1 -> "/DC1/rack1", 2 -> "/DC1/rack2", 3 -> "/DC1/rack2",
      4 -> "/DC2/rack1", 5 -> "/DC2/rack1", 6 -> "/DC2/rack2", 7 -> "/DC2/rack2")
    val numPartitions = 12
    val replicationFactor = 3
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor,
     true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor, verifyLeaderDistribution = false, verifyReplicasDistribution = false)
    checkMultiLevelReplicaDistribution(assignment, brokerRackMapping)
  }

  @Test
  def testAssignmentWithMlRackAwareWithUnevenRacks(): Unit = {
    val brokerRackMapping = Map(
      0 -> "/DC1/rack1", 1 -> "/DC1/rack1", 2 -> "/DC1/rack2", 3 -> "/DC1/rack3",
      4 -> "/DC2/rack1", 5 -> "/DC2/rack2", 6 -> "/DC2/rack2", 7 -> "/DC2/rack3")

    val numPartitions = 8
    val replicationFactor = 3
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor,
      true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor, verifyReplicasDistribution = false)
    checkMultiLevelReplicaDistribution(assignment, brokerRackMapping)
  }

  @Test
  def testAssignmentWith2ReplicasMlRackAware() = {
    val brokerRackMapping = Map(
      0 -> "/DC1/rack1", 1 -> "/DC1/rack2", 2 -> "/DC1/rack2", 3 -> "/DC1/rack3", 4 -> "/DC1/rack3", 5 -> "/DC1/rack1",
      6 -> "/DC2/rack1", 7 -> "/DC2/rack1", 8 -> "/DC2/rack2", 9 -> "/DC2/rack3", 10 -> "/DC2/rack3", 11 -> "/DC2/rack2")

    val numPartitions = 12
    val replicationFactor = 2
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
    checkMultiLevelReplicaDistribution(assignment, brokerRackMapping)
  }

  @Test
  def testMlRackAwareExpansion(): Unit = {
    val brokerRackMapping = Map(6 -> "/DC1/rack1", 7 -> "/DC1/rack1", 8 -> "/DC1/rack1", 9 -> "/DC2/rack2", 10 -> "/DC2/rack2", 11 -> "/DC2/rack2")
    val numPartitions = 12
    val replicationFactor = 2
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, 0 ,12, true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
    checkMultiLevelReplicaDistribution(assignment, brokerRackMapping)
  }

  @Test
  def testAssignmentWith2ReplicasMlRackAwareWith4PartitionsAnd4Brokers(): Unit = {
    val brokerRackMapping = Map(0 -> "/DC1/rack1", 1 -> "/DC1/rack2", 4 -> "/DC2/rack3", 7 -> "/DC2/rack5")
    val numPartitions = 4
    val replicationFactor = 2
    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor,
      true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions, replicationFactor)
    checkMultiLevelReplicaDistribution(assignment, brokerRackMapping)
  }

  @Test
  def testLargeNumberPartitionsAssignmentMl(): Unit = {
    val brokerRackMapping = Map(
      0 -> "/DC1/rack1", 1 -> "/DC1/rack2", 2 -> "/DC1/rack2", 3 -> "/DC1/rack3", 4 -> "/DC1/rack3", 5 -> "/DC1/rack1",
      6 -> "/DC2/rack1", 7 -> "/DC2/rack2", 8 -> "/DC2/rack2", 9 -> "/DC2/rack3", 10 -> "/DC2/rack1", 11 -> "/DC2/rack3")
    val numPartitions = 96
    val replicationFactor = 3

    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, true))
    checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size, numPartitions,
      replicationFactor)
    checkMultiLevelReplicaDistribution(assignment, brokerRackMapping)
  }

  @Test
  def testMoreReplicasThanMlRacks(): Unit = {
    // 4 racks
    val brokerRackMapping = Map(0 -> "/DC1/rack1", 1 -> "/DC1/rack2", 2 -> "/DC2/rack2", 3 -> "/DC2/rack3", 4 -> "/DC2/rack3", 5 -> "/DC2/rack2")
    val numPartitions = 6
    val replicationFactor = 5

    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor,
      true))
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.toIndexedSeq.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 until numPartitions)
      assertEquals(4, distribution.partitionRacks(partition).toSet.size)
  }

  @Test
  def testLessReplicasThanMlRacks(): Unit = {
    // 4 racks
    val brokerRackMapping = Map(0 -> "/DC1/rack1", 1 -> "/DC1/rack2", 2 -> "/DC2/rack2", 3 -> "/DC2/rack3", 4 -> "/DC2/rack3", 5 -> "/DC2/rack2")
    val numPartitions = 6
    val replicationFactor = 2

    val assignment = CoreUtils.replicaToBrokerAssignmentAsScala(AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions,
      replicationFactor, true))
    assertEquals(List.fill(assignment.size)(replicationFactor), assignment.values.toIndexedSeq.map(_.size))
    val distribution = getReplicaDistribution(assignment, brokerRackMapping)
    for (partition <- 0 to 5)
      assertEquals(2, distribution.partitionRacks(partition).toSet.size)
  }

}
