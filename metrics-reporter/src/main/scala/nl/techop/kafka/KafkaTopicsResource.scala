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
  **/

package nl.techop.kafka

import com.fasterxml.jackson.annotation.JsonProperty
import kafka.server.KafkaBroker

import java.util.{Collections, Properties}
import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}
import scala.jdk.CollectionConverters._

case class KafkaTopic(internal: Boolean, getPartitions: java.util.List[KafkaPartition], getTopicProps: Properties) {

  @JsonProperty(value = "isInternal")
  def isInternal = internal

}

case class KafkaPartition(getPartitionId: Int, getReplicas: java.util.List[Integer], getLeader: Int, getIsr: java.util.List[Integer])

@Path("/topics")
@Produces(Array(MediaType.APPLICATION_JSON))
class KafkaTopicsResource(server: KafkaBroker) {
  require(server != null, "server cannot be null")

  @GET
  def listTopics: java.util.Map[String, KafkaTopic] = {
    // metadataCache may not be initialized
    if (server.metadataCache == null) return Collections.emptyMap()

    val topics = server.metadataCache.getAllTopics()

    // just need a valid listener
    val listener = server.config.interBrokerListenerName
    val result =
      for {
        topicMetadata <- server.metadataCache.getTopicMetadata(topics, listener, true)
        topicName = topicMetadata.name
        internal = topicMetadata.isInternal
        kafkaPartitions = for {
          partitionMetadata <- topicMetadata.partitions.asScala
          partition = partitionMetadata.partitionIndex
          replicas = partitionMetadata.replicaNodes
          leader = partitionMetadata.leaderId
          isr = partitionMetadata.isrNodes
        } yield new KafkaPartition(partition, replicas, leader, isr) if kafkaPartitions.nonEmpty
      } yield (topicName, new KafkaTopic(internal, kafkaPartitions.asJava, null))

    result.toMap.asJava
  }

}

