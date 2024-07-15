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

package prometheus.metrics.reporting

import com.cloudera.kafka.prometheus.metrics.reporting.{PrometheusMetricsHandler, PrometheusMetricsServlet}
import kafka.api.{Both, SaslSetup}
import kafka.server.{KafkaBroker, KafkaConfig}
import kafka.utils.{JaasTestUtils, TestUtils}
import nl.techop.kafka.MetricsTestUtils.sendRequest
import nl.techop.kafka.{KafkaHttpMetricsReporter, MetricsIntegrationTestBase}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.eclipse.jetty.servlet.ServletContextHandler
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import scala.collection.Seq

import java.util.{Collections, Properties}
import scala.io.Source.fromInputStream

class CustomPrometheusMetricsHandler(groupManagerProvider: Option[() => GroupCoordinator])
  extends PrometheusMetricsHandler(groupManagerProvider) {

  override def handleGroupCommittedOffset(group: String, topic: String, partition: String, offset: Long): Unit = {
    assertEquals("PrometheusMetricsIntegrationTest-test-group", group)
    assertEquals("prom.test", topic)
    assertEquals("0", partition)
    assertEquals(100, offset)
  }
}

class CustomPrometheusKafkaHttpMetricsReporter extends KafkaHttpMetricsReporter {

  override def addPrometheusMetricsServlet(servletContextHandler: ServletContextHandler,
                                           groupManagerProvider: Option[() => GroupCoordinator]): Unit = {
    val prometheusMetricsServlet = new PrometheusMetricsServlet(new CustomPrometheusMetricsHandler(groupManagerProvider))
    KafkaYammerMetrics.defaultRegistry().addListener(prometheusMetricsServlet)
    addMetricsServlet(servletContextHandler, prometheusMetricsServlet, "/api/prometheus-metrics")
  }
}

// TODO: write and alternative configuration that tests the same but with SASL_SSL settings (basic auth is fine)
class PrometheusMetricsIntegrationTest extends MetricsIntegrationTestBase {

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    props.setProperty("kafka.metrics.reporters", classOf[CustomPrometheusKafkaHttpMetricsReporter].getName)
    this.props = props
    Seq(KafkaConfig.fromProps(props))
  }

  @Test
  def testPrometheusMetrics(): Unit = {
    TestUtils.createTopic(zkClient, "prom.test", 1, 1, servers, new Properties())
    TestUtils.generateAndProduceMessages(servers, "prom.test", 100)
    val consumer = TestUtils.createConsumer(bootstrapServers(), "PrometheusMetricsIntegrationTest-test-group")
    try {
      consumer.subscribe(Collections.singleton("prom.test"))
      TestUtils.consumeRecords(consumer, 100)
      consumer.commitSync()
      val response = sendRequest(s"http://localhost:${port}/api/prometheus-metrics")
      val body = fromInputStream(response.getEntity().getContent()).mkString
      assertEquals(200, response.getStatusLine.getStatusCode)
      assertTrue(body.contains("partition_log_endoffset{topic=\"prom.test\",partition=\"0\",} 100.0"))
    } finally {
      if (consumer != null) {
        consumer.close()
      }
    }
  }
}

class PrometheusMetricsIntegrationTestSASL extends MetricsIntegrationTestBase with SaslSetup {

  override protected def securityProtocol = SecurityProtocol.SASL_SSL

  override protected lazy val trustStoreFile = Some(TestUtils.tempFile("truststore", ".jks"))

  protected def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach {
      config =>
        config.setProperty("kafka.metrics.reporters", classOf[CustomPrometheusKafkaHttpMetricsReporter].getName)
    }
  }

  override def generateConfigs: Seq[KafkaConfig] = {

    val cfgs = TestUtils.createBrokerConfigs(1, zkConnectOrNull, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties)
    modifyConfigs(cfgs)
    this.props = cfgs.head
    cfgs.map(KafkaConfig.fromProps)
  }

  def produceMessages[B <: KafkaBroker](
                                         brokers: Seq[B],
                                         records: Seq[ProducerRecord[Array[Byte], Array[Byte]]],
                                         acks: Int = -1,
                                         props: Properties = null): Unit = {
    val producer = TestUtils.createProducer(TestUtils.bootstrapServers(brokers, ListenerName.forSecurityProtocol(securityProtocol)), securityProtocol = securityProtocol, trustStoreFile = trustStoreFile, saslProperties = clientSaslProperties, props = props)
    try {
      val futures = records.map(producer.send)
      futures.foreach(_.get)
    } finally {
      producer.close()
    }

    val topics = records.map(_.topic).distinct
    debug(s"Sent ${records.size} messages for topics ${topics.mkString(",")}")
  }


  def generateAndProduceMessages[B <: KafkaBroker](
                                                    brokers: Seq[B],
                                                    topic: String,
                                                    numMessages: Int,
                                                    acks: Int = -1,
                                                    props: Properties = null): Seq[String] = {
    val values = (0 until numMessages).map(x => s"test-$x")
    val intSerializer = new IntegerSerializer()
    val records = values.zipWithIndex.map { case (v, i) =>
      new ProducerRecord(topic, intSerializer.serialize(topic, i), v.getBytes)
    }
    produceMessages(brokers, records, acks, props)
    values
  }


  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    setUpSasl()
    super.setUp(testInfo)
  }

  def setUpSasl(): Unit = {
    startSasl(jaasSections(Seq("GSSAPI"), Some("GSSAPI"), Both, JaasTestUtils.KafkaServerContextName))
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  @Test
  def testPrometheusMetrics(): Unit = {
    TestUtils.createTopic(zkClient, "prom.test", 1, 1, servers, new Properties())
    println("Generating and producing messages")
    generateAndProduceMessages(servers, "prom.test", 100)
    println("Creating consumer")
    val consumer = TestUtils.createConsumer(bootstrapServers(), "PrometheusMetricsIntegrationTest-test-group", trustStoreFile = trustStoreFile, securityProtocol = securityProtocol, saslProperties = clientSaslProperties)
    try {
      consumer.subscribe(Collections.singleton("prom.test"))
      TestUtils.consumeRecords(consumer, 100)
      consumer.commitSync()
      val response = sendRequest(s"http://localhost:${port}/api/prometheus-metrics")
      val body = fromInputStream(response.getEntity().getContent()).mkString
      assertEquals(200, response.getStatusLine.getStatusCode)
      assertTrue(body.contains("partition_log_endoffset{topic=\"prom.test\",partition=\"0\",} 100.0"))
    } finally {
      if (consumer != null) {
        consumer.close()
      }
    }
  }
}