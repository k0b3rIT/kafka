/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import scala.collection.{Map, Seq}
import org.junit.jupiter.api.Assertions._

import scala.annotation.nowarn


trait MultiLevelRackAwareTest {

  @nowarn("cat=deprecation")
  def checkMultiLevelReplicaDistribution(assignment: Map[Int, Seq[Int]],
                                         brokerRackMapping: Map[Int, String]): Unit = {

    val structuredBrokerRackMap = brokerRackMapping
      .mapValues(_.split("/").toList.filter(_.nonEmpty))
      .toMap

    val maxLevels = structuredBrokerRackMap.values.map(_.size).head

    for (level <- 0 to maxLevels) {
      val distribution = getMultiLevelReplicaDistribution(assignment, structuredBrokerRackMap, level)

      val leastLoaded = distribution.values.map(_.size).min
      val mostLoaded = distribution.values.map(_.size).max

      assertTrue(mostLoaded - leastLoaded  <= 1)
    }


  }

  @nowarn("cat=unused")
  def getMultiLevelReplicaDistribution(assignment : Map[Int, Seq[Int]],
                                       structuredBrokerRackMap: Map[Int, List[String]],
                                       level: Int) = {


    import scala.collection.compat._

    val mapOfBrokersToRacks = structuredBrokerRackMap
      .toSeq
      .groupMap(_._2.take(level))(_._1)
      .toSeq
      .map( _.swap )
      .toMap

    val racksToPartitions = assignment
      .toSeq
      .flatMap { case (part, brIds) => brIds.map( (part, _) ) }
      .map { case (part, brId) => (part, mapOfBrokersToRacks.find { case(brIds, rack) => brIds.contains(brId) }.get._2) }
      .groupMap(_._2)(_._1)


    racksToPartitions
  }

}