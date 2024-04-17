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

package producer

import java.io.ByteArrayOutputStream
import com.cloudera.kafka.producer.ProducerDetailsServlet
import com.cloudera.kafka.producer.ProducerDetailsServlet.{PRODUCER, TOPIC}
import com.cloudera.kafka.server.ProducerStats
import com.fasterxml.jackson.databind.ObjectMapper

import javax.servlet.http.HttpServletResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.eclipse.jetty.server.{Request, Response}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.{any, anyInt, anyString, eq => _eq}
import org.mockito.Mockito
import org.mockito.Mockito.mock

class ProducerDetailsServletTest {

  val producerStats: ProducerStats = Mockito.spy(new ProducerStats(1, 1, true, "", mock(classOf[KafkaMetricsGroup])))
  val producerDetailsServlet: ProducerDetailsServlet = Mockito.spy(new ProducerDetailsServlet(null))
  val httpServletRequest: Request = Mockito.spy(new Request(null, null))
  val httpServletResponse: Response = Mockito.spy(new Response(null, null))

  @Test
  def testProducerDetailsServlet_filterByTopic(): Unit = {
    val testTopic = "testTopic"
    val testPartition  = "1"
    val outputStream = new ByteArrayOutputStream
    val allClientRelations = Set(("clientId-1", new TopicPartition("otherTopic", 2)),
      ("clientId-2", new TopicPartition(testTopic, testPartition.toInt)),
      ("clientId-3", new TopicPartition(testTopic, testPartition.toInt)),
      ("clientId-4", new TopicPartition(testTopic, 2)))

    Mockito.doAnswer(_ => testTopic).when(httpServletRequest).getParameter(ProducerDetailsServlet.TOPIC)
    Mockito.doAnswer(_ => null).when(httpServletRequest).getParameter(ProducerDetailsServlet.PRODUCER)
    Mockito.doAnswer(_ => outputStream).when(producerDetailsServlet).getOutputStream(any())
    Mockito.doAnswer(_ => allClientRelations).when(producerStats).getAllClientRelations()

    producerDetailsServlet.doGet(httpServletRequest, httpServletResponse)


    val objectMapper = new ObjectMapper()
    val rootNode = objectMapper.readTree(outputStream.toString)

    val testTopicNode = rootNode.get("testTopic")
    val otherTopic = rootNode.get("otherTopic")
    val partitionOneNode = testTopicNode.get("1")
    val partitionOneString = testTopicNode.get("1").toString
    val partitionTwoNode = testTopicNode.get("2")
    val partitionTwoString = testTopicNode.get("2").toString

    assertTrue(partitionOneNode.size() == 2 && partitionOneString.contains("clientId-2") && partitionOneString.contains("clientId-3"))
    assertTrue(partitionTwoNode.size() == 1 && partitionTwoString.contains("clientId-4"))
    assertNull(otherTopic)
  }

  @Test
  def testProducerDetailsServlet_filterByProducer(): Unit = {
    val outputStream = new ByteArrayOutputStream

    val allClientRelations = Set(
      ("clientId-1", new TopicPartition("testTopic-1", 1)),
      ("clientId-1", new TopicPartition("testTopic-3", 1)),
      ("clientId-2", new TopicPartition("testTopic-1", 1)),
      ("clientId-2", new TopicPartition("testTopic-2", 1)),
      ("clientId-2", new TopicPartition("testTopic-3", 2)),
    )

    Mockito.doAnswer(_ => null).when(httpServletRequest).getParameter(ProducerDetailsServlet.TOPIC)
    Mockito.doAnswer(_ => "clientId-1").when(httpServletRequest).getParameter(ProducerDetailsServlet.PRODUCER)
    Mockito.doAnswer(_ => outputStream).when(producerDetailsServlet).getOutputStream(any())
    Mockito.doAnswer(_ => allClientRelations).when(producerStats).getAllClientRelations()

    producerDetailsServlet.doGet(httpServletRequest, httpServletResponse)

    val objectMapper = new ObjectMapper()
    val rootNode = objectMapper.readTree(outputStream.toString)
    val testTopic1Partition1Node = rootNode.get("testTopic-1").get("1")

    assertNotNull(rootNode.get("testTopic-1"))
    assertEquals(1, testTopic1Partition1Node.size())
    assertEquals("clientId-1", testTopic1Partition1Node.get(0).asText())
    assertNull(rootNode.get("testTopic-2"))
    assertNotNull(rootNode.get("testTopic-3"))
  }

  @Test
  def testProducerDetailsServlet_filterByTopicAndProducer(): Unit = {
    val outputStream = new ByteArrayOutputStream

    val allClientRelations = Set(
      ("clientId-1", new TopicPartition("testTopic-1", 1)),
      ("clientId-1", new TopicPartition("testTopic-3", 1)),
      ("clientId-2", new TopicPartition("testTopic-1", 1)),
      ("clientId-2", new TopicPartition("testTopic-2", 1)),
      ("clientId-2", new TopicPartition("testTopic-3", 2)),
    )

    Mockito.doAnswer(_ => "testTopic-1").when(httpServletRequest).getParameter(ProducerDetailsServlet.TOPIC)
    Mockito.doAnswer(_ => "clientId-1").when(httpServletRequest).getParameter(ProducerDetailsServlet.PRODUCER)
    Mockito.doAnswer(_ => outputStream).when(producerDetailsServlet).getOutputStream(any())
    Mockito.doAnswer(_ => allClientRelations).when(producerStats).getAllClientRelations()

    producerDetailsServlet.doGet(httpServletRequest, httpServletResponse)

    val objectMapper = new ObjectMapper()
    val rootNode = objectMapper.readTree(outputStream.toString)
    val testTopic1Partition1Node = rootNode.get("testTopic-1").get("1")

    assertNotNull(rootNode.get("testTopic-1"))
    assertEquals(1, testTopic1Partition1Node.size())
    assertEquals("clientId-1", testTopic1Partition1Node.get(0).asText())
    assertNull(rootNode.get("testTopic-2"))
    assertNull(rootNode.get("testTopic-3"))
  }

  @Test
  def testMissingParamError(): Unit = {
    Mockito.doAnswer(_ => new ByteArrayOutputStream()).when(producerDetailsServlet).getOutputStream(any())
    Mockito.doAnswer(_ => Set()).when(producerStats).getAllClientRelations()
    Mockito.doAnswer(_ => null).when(httpServletRequest).getParameter(ProducerDetailsServlet.TOPIC)
    Mockito.doAnswer(_ => null).when(httpServletRequest).getParameter(ProducerDetailsServlet.PRODUCER)

    producerDetailsServlet.doGet(httpServletRequest, httpServletResponse)

    Mockito.verify(httpServletResponse, Mockito.times(1))
      .sendError(_eq(HttpServletResponse.SC_BAD_REQUEST), _eq(s"At least one of query params $TOPIC or $PRODUCER is required"))
  }

  @BeforeEach
  def setup(): Unit = {
    Mockito.reset(producerStats, producerDetailsServlet, httpServletRequest, httpServletResponse)
    Mockito.doAnswer(_ => Some(producerStats)).when(producerDetailsServlet).getProducerStats
    Mockito.doNothing().when(httpServletResponse).setStatus(anyInt())
    Mockito.doNothing().when(httpServletResponse).setContentType(anyString())
    Mockito.doNothing().when(httpServletResponse).sendError(anyInt(), anyString())
  }

}
