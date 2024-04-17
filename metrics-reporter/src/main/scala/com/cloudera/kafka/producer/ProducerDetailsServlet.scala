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

package com.cloudera.kafka.producer

import com.cloudera.kafka.server.ProducerStats
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import kafka.server.KafkaBroker
import kafka.utils.Logging

import java.io.OutputStream
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

class ProducerDetailsServlet(broker: KafkaBroker) extends HttpServlet with Logging {

  private val DEFAULT_JSON_FACTORY = new JsonFactory(new ObjectMapper)

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    import ProducerDetailsServlet._

    resp.setStatus(200)
    resp.setContentType("application/json")

    val queryTopic = Option(req.getParameter(TOPIC))
    val queryProducer = Option(req.getParameter(PRODUCER))

    if ((queryTopic.isEmpty || queryTopic.get.isEmpty) &&
        (queryProducer.isEmpty || queryProducer.get.isEmpty)){
      resp.sendError(HttpServletResponse.SC_BAD_REQUEST, s"At least one of query params $TOPIC or $PRODUCER is required")
      return
    }

    val json = DEFAULT_JSON_FACTORY.createGenerator(getOutputStream(resp))
    json.writeStartObject()

    val filteredRelations = getProducerStats match {
      case Some(producerStats) => producerStats.getAllClientRelations()
        .filter{case (producer, _) => queryProducer.isEmpty || producer.equals(queryProducer.get)}
        .filter{case (_, topicPartition) => queryTopic.isEmpty || topicPartition.topic().equals(queryTopic.get)}
        .groupBy(_._2.topic())
        .transform((_, v) => v.groupBy(_._2))
      case None => Map.empty
    }

    filteredRelations.foreach(relationsByTopic => {
      json.writeFieldName(relationsByTopic._1)
      json.writeStartObject()

      relationsByTopic._2.foreach(relation => {
        json.writeFieldName(s"${relation._1.partition()}")
        json.writeStartArray()
        relation._2.foreach{case (clientId, _) => json.writeString(clientId)}
        json.writeEndArray()
      })

      json.writeEndObject()
    })

    json.writeEndObject()
    json.close()
  }

  // Visible for testing
  def getProducerStats: Option[ProducerStats] = {
    broker.replicaManager.producerStats
  }

  // Visible for testing
  def getOutputStream(resp: HttpServletResponse): OutputStream = {
    resp.getOutputStream
  }
}

object ProducerDetailsServlet {
  val TOPIC = "topic"
  val PRODUCER = "producer"
}
