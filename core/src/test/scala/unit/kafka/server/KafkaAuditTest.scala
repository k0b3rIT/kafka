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

package unit.kafka.server

import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.controller.{KafkaController, ReplicaAssignment}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server._
import kafka.server.metadata.ZkConfigRepository
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.{ReassignablePartition, ReassignableTopic}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopicCollection
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.server.auditor.Auditor.AuditInformation
import org.apache.kafka.server.auditor.TopicEvent.AuditedTopic
import org.apache.kafka.server.auditor.{Auditor, TopicEvent}
import org.apache.kafka.server.authorizer.{AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion}
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs}
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentMatchers.{eq => emeq, _}
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Mockito}

import java.net.InetAddress
import java.util.Collections
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class KafkaAuditTest {

  private val brokerId = 1
  private val properties = TestUtils.createBrokerConfig(brokerId, "zk")
  properties.put(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, MetadataVersion.latestTesting().version())
  private val kafkaConfig = new KafkaConfig(properties)

  private val requestChannel: RequestChannel = Mockito.mock(classOf[RequestChannel])
  private val requestChannelMetrics: RequestChannel.Metrics = Mockito.mock(classOf[RequestChannel.Metrics])
  private val replicaManager: ReplicaManager = Mockito.mock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = Mockito.mock(classOf[GroupCoordinator])
  private val txnCoordinator: TransactionCoordinator = Mockito.mock(classOf[TransactionCoordinator])
  private val controller: KafkaController = Mockito.mock(classOf[KafkaController])
  private val zkClient: KafkaZkClient = Mockito.mock(classOf[KafkaZkClient])
  private val autoTopicCreationManager: AutoTopicCreationManager = Mockito.mock(classOf[AutoTopicCreationManager])
  private val metrics = new Metrics()
  private val metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
  private val brokerEpochManager: ZkBrokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
  private val clientQuotaManager: ClientQuotaManager = Mockito.mock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = Mockito.mock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = Mockito.mock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    clientControllerQuotaManager, replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, None)
  private val fetchManager: FetchManager = Mockito.mock(classOf[FetchManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime
  private val clientId = ""
  private val testTopicName: String = "test-topic"
  private val testTopicUuid = Uuid.randomUuid()

  @AfterEach
  def tearDown(): Unit = {
    quotas.shutdown()
    TestUtils.clearYammerMetrics()
    metrics.close()
  }

  @Test
  def testCaptureTopicCreateEventWhenControllerNotActive(): Unit = {
    val request = buildCreateTopicsRequest(testTopicName, 3, 1)
    val auditor: Auditor = mock(classOf[Auditor])
    val kafkaApis = buildKafkaApis(auditor)
    val authInfo = buildTopicAuthInfoMap(AclOperation.CREATE, testTopicName, Errors.NOT_CONTROLLER)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName)), TopicEvent.EventType.CREATE)

    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicCreateEvent(): Unit = {
    setupBasicMetadataCache("exsting-topic", 1, Uuid.randomUuid()) // just initialize the metadata with something
    val request = buildCreateTopicsRequest(testTopicName, 3, 1)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager: ZkAdminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val kafkaApis = buildKafkaApis(auditor, adminManager)
    val authInfo = buildTopicAuthInfoMap(AclOperation.CREATE, testTopicName, Errors.NONE)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName, 3, 1)), TopicEvent.EventType.CREATE)

    when(zkClient.topicExists(emeq(testTopicName))).thenReturn(false)
    when(zkClient.getTopicIdsForTopics(emeq(Set(testTopicName)))).thenReturn(Map(testTopicName -> testTopicUuid))
    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // update the metadata cache to emulate that the topic got created
    setupBasicMetadataCache(testTopicName, 3, testTopicUuid)
    // execute the callback which calls the auditor
    adminManager.tryCompleteDelayedTopicOperations(testTopicName)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicCreateEventWithDefaults(): Unit = {
    setupBasicMetadataCache("exsting-topic", 1, Uuid.randomUuid()) // just initialize the metadata with something
    val request = buildCreateTopicsRequest(testTopicName, -1, -1)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager: ZkAdminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val kafkaApis = buildKafkaApis(auditor, adminManager)
    val authInfo = buildTopicAuthInfoMap(AclOperation.CREATE, testTopicName, Errors.NONE)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName, 1, 1)), TopicEvent.EventType.CREATE)

    when(zkClient.topicExists(emeq(testTopicName))).thenReturn(false)
    when(zkClient.getTopicIdsForTopics(emeq(Set(testTopicName)))).thenReturn(Map(testTopicName -> testTopicUuid))
    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // update the metadata cache to emulate that the topic got created
    setupBasicMetadataCache(testTopicName, 1, testTopicUuid)
    // execute the callback which calls the auditor
    adminManager.tryCompleteDelayedTopicOperations(testTopicName)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicCreateEventWhenTopicExists(): Unit = {
    setupBasicMetadataCache(testTopicName, 1, testTopicUuid) // topic already exists in metadata
    val request = buildCreateTopicsRequest(testTopicName, 3, 1)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager: ZkAdminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val kafkaApis = buildKafkaApis(auditor, adminManager)
    val authInfo = buildTopicAuthInfoMap(AclOperation.CREATE, testTopicName, Errors.TOPIC_ALREADY_EXISTS)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName)), TopicEvent.EventType.CREATE)

    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicCreateEventWhenNotAuthorized(): Unit = {
    setupBasicMetadataCache(testTopicName, 1, testTopicUuid) // topic already exists in metadata
    val request = buildCreateTopicsRequest(testTopicName, 3, 1)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val kafkaApis = buildKafkaApis(auditor, adminManager, Option(authorizer))
    val authInfo = buildTopicAuthInfoMap(AclOperation.CREATE, testTopicName, Errors.TOPIC_AUTHORIZATION_FAILED)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName)), TopicEvent.EventType.CREATE)

    when(authorizer.authorize(emeq(request.context), any()))
      .thenReturn(Collections.singletonList(AuthorizationResult.DENIED))
    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicCreateEventInTopicMetadata(): Unit = {
    setupBasicMetadataCache("exsting-topic", 1, Uuid.randomUuid()) // just initialize the metadata with something
    val request = buildTopicMetadataRequest(testTopicName)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val kafkaApis = buildKafkaApis(auditor, adminManager)
    val authInfo = buildTopicAuthInfoMap(AclOperation.CREATE, testTopicName, Errors.LEADER_NOT_AVAILABLE)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName)), TopicEvent.EventType.CREATE)
    val result = new MetadataResponseData.MetadataResponseTopic()
      .setName(testTopicName)
      .setPartitions(Collections.emptyList())
      .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
    val quota: ControllerMutationQuota = mock(classOf[ControllerMutationQuota])

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newPermissiveQuotaFor(emeq(request))).thenReturn(quota)
    when(autoTopicCreationManager.createTopics(emeq(Set(testTopicName)), emeq(quota), emeq(Some(request.context)))).thenReturn(Seq(result))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // update the metadata cache to emulate that the topic got created
    setupBasicMetadataCache(testTopicName, 3, testTopicUuid)
    // execute the callback which calls the auditor
    adminManager.tryCompleteDelayedTopicOperations(testTopicName)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureInternalTopicCreateEventInTopicMetadata(): Unit = {
    setupBasicMetadataCache("exsting-topic", 1, Uuid.randomUuid())
    val newInternalTopic = "__transaction_state"
    val request = buildTopicMetadataRequest(newInternalTopic)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val kafkaApis = buildKafkaApis(auditor, adminManager)
    val authInfo = buildTopicAuthInfoMap(AclOperation.CREATE, newInternalTopic, Errors.LEADER_NOT_AVAILABLE)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(newInternalTopic)), TopicEvent.EventType.CREATE)
    val result = new MetadataResponseData.MetadataResponseTopic()
      .setName(newInternalTopic)
      .setPartitions(Collections.emptyList())
      .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
    val quota: ControllerMutationQuota = mock(classOf[ControllerMutationQuota])

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newPermissiveQuotaFor(emeq(request))).thenReturn(quota)
    when(autoTopicCreationManager.createTopics(emeq(Set(newInternalTopic)), emeq(quota), emeq(Some(request.context)))).thenReturn(Seq(result))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // update the metadata cache to emulate that the topic got created
    setupBasicMetadataCache(testTopicName, 3, testTopicUuid)
    // execute the callback which calls the auditor
    adminManager.tryCompleteDelayedTopicOperations(testTopicName)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicCreateEventInFindCoordinator(): Unit = {
    setupBasicMetadataCache("exsting-topic", 1, Uuid.randomUuid())
    val newInternalTopic = "__consumer_offsets"
    val request = buildFindCoordinatorRequest()
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val kafkaApis = buildKafkaApis(auditor, adminManager)
    val authInfo = buildTopicAuthInfoMap(AclOperation.CREATE, newInternalTopic, Errors.LEADER_NOT_AVAILABLE)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(newInternalTopic)), TopicEvent.EventType.CREATE)
    val result = new MetadataResponseData.MetadataResponseTopic()
      .setName(newInternalTopic)
      .setPartitions(Collections.emptyList())
      .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
    val quota: ControllerMutationQuota = mock(classOf[ControllerMutationQuota])

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newPermissiveQuotaFor(emeq(request))).thenReturn(quota)
    when(autoTopicCreationManager.createTopics(emeq(Set(newInternalTopic)), emeq(quota), emeq(None))).thenReturn(Seq(result))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // update the metadata cache to emulate that the topic got created
    setupBasicMetadataCache(testTopicName, 3, testTopicUuid)
    // execute the callback which calls the auditor
    adminManager.tryCompleteDelayedTopicOperations(testTopicName)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicDeleteEventWhenControllerNotActive(): Unit = {
    val request = buildDeleteTopicsRequest(testTopicName)
    val auditor: Auditor = mock(classOf[Auditor])
    val kafkaApis = buildKafkaApis(auditor)
    val authInfo = buildTopicAuthInfoMap(AclOperation.DELETE, testTopicName, Errors.NOT_CONTROLLER)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName)), TopicEvent.EventType.DELETE)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicDeleteEvent(): Unit = {
    setupBasicMetadataCache(testTopicName, 3, testTopicUuid)
    val request = buildDeleteTopicsRequest(testTopicName)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager: ZkAdminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val kafkaApis = buildKafkaApis(auditor, adminManager)
    val authInfo = buildTopicAuthInfoMap(AclOperation.DELETE, testTopicName, Errors.NONE)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName)), TopicEvent.EventType.DELETE)

    when(zkClient.topicExists(emeq(testTopicName))).thenReturn(true)
    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    updateMetadataCacheWithDelete(testTopicName, 3, testTopicUuid)
    adminManager.tryCompleteDelayedTopicOperations(testTopicName)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicDeleteEventWhenTopicDoesntExist(): Unit = {
    setupBasicMetadataCache("some-topic", 3, Uuid.randomUuid())
    val request = buildDeleteTopicsRequest(testTopicName)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager: ZkAdminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val kafkaApis = buildKafkaApis(auditor, adminManager)
    val authInfo = buildTopicAuthInfoMap(AclOperation.DELETE, testTopicName, Errors.UNKNOWN_TOPIC_OR_PARTITION)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName)), TopicEvent.EventType.DELETE)

    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicDeleteEventWhenNotAuthorized(): Unit = {
    setupBasicMetadataCache(testTopicName, 1, testTopicUuid) // topic already exists in metadata
    val request = buildDeleteTopicsRequest(testTopicName)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val kafkaApis = buildKafkaApis(auditor, adminManager, Option(authorizer))
    val authInfo = buildTopicAuthInfoMap(AclOperation.DELETE, testTopicName, Errors.TOPIC_AUTHORIZATION_FAILED)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName)), TopicEvent.EventType.DELETE)

    when(authorizer.authorize(emeq(request.context), any()))
      .thenReturn(Collections.singletonList(AuthorizationResult.DENIED))
    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCaptureTopicDeleteEventWhenDeletionDisabled(): Unit = {
    val properties = TestUtils.createBrokerConfig(brokerId, "zk")
    properties.put(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, MetadataVersion.latestTesting().version())
    properties.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, false.toString)
    val kafkaConfig = new KafkaConfig(properties)

    setupBasicMetadataCache(testTopicName, 1, testTopicUuid) // topic already exists in metadata
    val request = buildDeleteTopicsRequest(testTopicName)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val kafkaApis = buildKafkaApis(auditor, adminManager, None, kafkaConfig)
    val authInfo = buildTopicAuthInfoMap(AclOperation.DELETE, testTopicName, Errors.TOPIC_DELETION_DISABLED)
    val event = new TopicEvent(Collections.singleton(new AuditedTopic(testTopicName)), TopicEvent.EventType.DELETE)

    when(controller.isActive).thenReturn(true)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    when(authorizer.authorize(any[RequestContext], anyList())).thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testAlterPartitionsReassignmentRequest(): Unit = {
    val topicWithSinglePartition = "topicWithSinglePartition"
    val topicWith2PartitionsDifferentRF = "topicWith2PartitionsDifferentRF"
    val topicWith2PartitionsOneFailed = "topicWith2PartitionsOneFailed"
    val topicWithAllPartitionsFailed = "topicWithAllPartitionsFailed"

    def buildAlterPartitionReassignmentsRequest(): RequestChannel.Request = {

      val requestData = new AlterPartitionReassignmentsRequestData()
        .setTopics(Seq(
          new ReassignableTopic() // Single partition with RF=3
            .setName(topicWithSinglePartition)
            .setPartitions(Seq(
              new ReassignablePartition()
                .setPartitionIndex(0)
                .setReplicas(Seq[Integer](1, 2, 3).asJava)
            ).asJava),
          new ReassignableTopic()
            .setName(topicWith2PartitionsDifferentRF) // 2 partitions with different RF, we expect the max to be applied
            .setPartitions(Seq(
              new ReassignablePartition()
                .setPartitionIndex(0)
                .setReplicas(Seq[Integer](1, 2, 3).asJava),
              new ReassignablePartition()
                .setPartitionIndex(1)
                .setReplicas(Seq[Integer](1, 2).asJava)
            ).asJava),
          new ReassignableTopic() // 2 partitions, but one will fail, we expect the successful to be applied
            .setName(topicWith2PartitionsOneFailed)
            .setPartitions(Seq(
              new ReassignablePartition()
                .setPartitionIndex(0)
                .setReplicas(Seq[Integer](1, 2, 3).asJava),
              new ReassignablePartition()
                .setPartitionIndex(1)
                .setReplicas(Seq[Integer](1, 2, 3).asJava)
            ).asJava),
          new ReassignableTopic() // Single partition with RF=3
            .setName(topicWithAllPartitionsFailed)
            .setPartitions(Seq(
              new ReassignablePartition()
                .setPartitionIndex(0)
                .setReplicas(Seq[Integer](1, 2, 3).asJava)
            ).asJava)
        ).asJava)
      val request = new AlterPartitionReassignmentsRequest.Builder(requestData)
        .build(requestData.highestSupportedVersion())
      buildRequest(request)
    }

    def buildAlterPartitionsAuthInfo(): java.util.Map[AclOperation, java.util.Set[AuditInformation]] = {
      Map(
        AclOperation.ALTER ->
          Set(
            new Auditor.AuditInformation(
              new ResourcePattern(ResourceType.TOPIC, topicWithSinglePartition,
                PatternType.LITERAL),
              Errors.NONE.code
            ),
            new Auditor.AuditInformation(
              new ResourcePattern(ResourceType.TOPIC, topicWith2PartitionsDifferentRF,
                PatternType.LITERAL),
              Errors.NONE.code
            ),
            new Auditor.AuditInformation(
              new ResourcePattern(ResourceType.TOPIC, topicWith2PartitionsOneFailed,
                PatternType.LITERAL),
              Errors.NONE.code
            ),
            new Auditor.AuditInformation(
              new ResourcePattern(ResourceType.TOPIC, topicWith2PartitionsOneFailed,
                PatternType.LITERAL),
              Errors.UNKNOWN_SERVER_ERROR.code
            ),
            new Auditor.AuditInformation(
              new ResourcePattern(ResourceType.TOPIC, topicWithAllPartitionsFailed,
                PatternType.LITERAL),
              Errors.UNKNOWN_SERVER_ERROR.code
            )
          ).asJava
      ).asJava
    }

    val properties = TestUtils.createBrokerConfig(brokerId, "zk")
    properties.put(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, MetadataVersion.latestTesting().version())
    val kafkaConfig = new KafkaConfig(properties)

    setupBasicMetadataCache(Map(
      topicWithSinglePartition -> (1, Uuid.randomUuid()),
      topicWith2PartitionsDifferentRF -> (2, Uuid.randomUuid()),
      topicWith2PartitionsOneFailed -> (3, Uuid.randomUuid()),
      topicWithAllPartitionsFailed -> (4, Uuid.randomUuid())))
    val request = buildAlterPartitionReassignmentsRequest()
    val authInfo = buildAlterPartitionsAuthInfo()
    val topicWithSinglePartitionReassignmentResult = Map(new TopicPartition(topicWithSinglePartition, 0) -> ApiError.NONE)
    val topicWith2PartitionsDifferentRFReassignmentResult = Map(
      new TopicPartition(topicWith2PartitionsDifferentRF, 0) -> ApiError.NONE,
      new TopicPartition(topicWith2PartitionsDifferentRF, 1) -> ApiError.NONE)
    val topicWith2PartitionsOneFailedReassignmentResult = Map(
      new TopicPartition(topicWith2PartitionsOneFailed, 0) -> ApiError.NONE,
      new TopicPartition(topicWith2PartitionsOneFailed, 1) -> ApiError.fromThrowable(Errors.UNKNOWN_SERVER_ERROR.exception()))
    val topicWithAllPartitionsFailedReassignmentResult = Map(
      new TopicPartition(topicWithAllPartitionsFailed, 0) -> ApiError.fromThrowable(Errors.UNKNOWN_SERVER_ERROR.exception()))
    val expectedTopicPartitionMap = Map(
      new TopicPartition(topicWithSinglePartition, 0) -> Option(List(1, 2, 3)),
      new TopicPartition(topicWith2PartitionsDifferentRF, 0) -> Option(List(1, 2, 3)),
      new TopicPartition(topicWith2PartitionsDifferentRF, 1) -> Option(List(1, 2)),
      new TopicPartition(topicWith2PartitionsOneFailed, 0) -> Option(List(1, 2, 3)),
      new TopicPartition(topicWith2PartitionsOneFailed, 1) -> Option(List(1, 2, 3)),
      new TopicPartition(topicWithAllPartitionsFailed, 0) -> Option(List(1, 2, 3)),
    )
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val kafkaApis = buildKafkaApis(auditor, adminManager, None, kafkaConfig)
    val event = new TopicEvent(Set(AuditedTopic.withReplicationFactor(topicWithSinglePartition, 3),
      AuditedTopic.withReplicationFactor(topicWith2PartitionsDifferentRF, 3),
      AuditedTopic.withReplicationFactor(topicWith2PartitionsOneFailed, 3),
      AuditedTopic.withReplicationFactor(topicWithAllPartitionsFailed, 3)).asJava,
      TopicEvent.EventType.REPLICATION_FACTOR_CHANGE)

    val callbackCapture: ArgumentCaptor[Either[Map[TopicPartition, ApiError], ApiError] => Unit] = ArgumentCaptor.forClass(classOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit])

    when(authorizer.authorize(any(), any()))
      .thenReturn(Collections.singletonList(AuthorizationResult.ALLOWED))
    when(
      controller.alterPartitionReassignments(emeq(expectedTopicPartitionMap), callbackCapture.capture())
    ).thenAnswer(_ => callbackCapture.getValue.apply(Left(
      topicWithSinglePartitionReassignmentResult ++ topicWith2PartitionsDifferentRFReassignmentResult ++
        topicWith2PartitionsOneFailedReassignmentResult ++ topicWithAllPartitionsFailedReassignmentResult))
    )
    doNothing().when(auditor).audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }

  @Test
  def testCapturePartitionCreateEvent(): Unit = {
    setupBasicMetadataCache(testTopicName, 1, testTopicUuid) // just initialize the metadata with something
    val request = buildCreatePartitionRequest(testTopicName, 2)
    val auditor: Auditor = mock(classOf[Auditor])
    val adminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient)
    val kafkaApis = buildKafkaApis(auditor, adminManager)
    val authInfo = buildTopicAuthInfoMap(AclOperation.ALTER, testTopicName, Errors.NONE)
    val event = new TopicEvent(Collections.singleton(AuditedTopic.withPartitionNumber(testTopicName, 2)),
      TopicEvent.EventType.PARTITION_CHANGE)

    when(zkClient.topicExists(emeq(testTopicName))).thenReturn(true)
    when(zkClient.getAllBrokersInCluster).thenReturn(Seq(Broker(brokerId, Seq(), None)))
    when(zkClient.getFullReplicaAssignmentForTopics(any()))
      .thenReturn(Map(new TopicPartition(testTopicName, 0) -> ReplicaAssignment(Seq(brokerId))))
    when(zkClient.getTopicIdsForTopics(emeq(Set(testTopicName))))
      .thenReturn(Map(testTopicName -> testTopicUuid))

    when(controller.isActive).thenReturn(true)
    when(controller.isTopicQueuedForDeletion(testTopicName)).thenReturn(false)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(0)
    when(clientControllerQuotaManager.newQuotaFor(emeq(request), anyShort())).thenReturn(mock(classOf[ControllerMutationQuota]))
    auditor.audit(emeq(event), emeq(request.context), emeq(authInfo))

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    setupBasicMetadataCache(testTopicName, 3, testTopicUuid) // simulate added partitions
    // execute the callback which calls the auditor
    adminManager.tryCompleteDelayedTopicOperations(testTopicName)
    // verify that the auditor has been called
    verify(auditor, times(1)).audit(emeq(event), emeq(request.context), emeq(authInfo))
  }


  private def buildKafkaApis(auditor: Auditor,
                             adminManager: ZkAdminManager = new ZkAdminManager(kafkaConfig, metrics, metadataCache, zkClient),
                             authorizerOpt: Option[Authorizer] = None,
                             kafkaConfig: KafkaConfig = kafkaConfig): KafkaApis = {

    val metadataSupport = ZkSupport(adminManager, controller, zkClient, None, metadataCache, brokerEpochManager)
    val listenerType = ListenerType.ZK_BROKER
    new KafkaApis(requestChannel,
      metadataSupport,
      replicaManager,
      groupCoordinator,
      txnCoordinator,
      autoTopicCreationManager,
      brokerId,
      kafkaConfig,
      ZkConfigRepository.apply(zkClient),
      metadataCache,
      metrics,
      authorizerOpt,
      quotas,
      fetchManager,
      brokerTopicStats,
      clusterId,
      time,
      mock(classOf[DelegationTokenManager]),
      new SimpleApiVersionManager(
        listenerType,
        ApiKeys.apisForListener(listenerType).asScala.toSet,
        BrokerFeatures.defaultSupportedFeatures(true),
        true,
        false,
        () => new FinalizedFeatures(MetadataVersion.latestTesting(), Collections.emptyMap[String, java.lang.Short], 0, false)),
      None,
      List(auditor)
    )
  }

  private def buildTopicAuthInfoMap(aclOperation: AclOperation,
                                    topicName: String,
                                    error: Errors): java.util.Map[AclOperation, java.util.Set[AuditInformation]] = {
    val resource = new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL)
    Map(aclOperation -> Collections.singleton(
      new AuditInformation(resource, error.code))).asJava
  }

  private def buildCreateTopicsRequest(topicName: String, numPartitions: Int, replicationFactor: Int): RequestChannel.Request = {
    val creatableTopic = new CreateTopicsRequestData.CreatableTopic()
      .setName(topicName)
      .setNumPartitions(numPartitions)
      .setReplicationFactor(replicationFactor.toShort)
    val createTopicsRequestData = new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(Collections.singleton(creatableTopic).iterator))
    val createTopicsRequest = new CreateTopicsRequest.Builder(createTopicsRequestData)
      .build(createTopicsRequestData.highestSupportedVersion)
    buildRequest(createTopicsRequest)
  }

  private def buildTopicMetadataRequest(topicName: String): RequestChannel.Request = {
    val topicMetadataRequestData = new MetadataRequestData()
      .setTopics(Collections.singletonList(new MetadataRequestTopic().setName(topicName)))
      .setAllowAutoTopicCreation(true)
    val topicMetadataRequest = new MetadataRequest.Builder(topicMetadataRequestData)
      .build(topicMetadataRequestData.highestSupportedVersion())
    buildRequest(topicMetadataRequest)
  }

  private def buildFindCoordinatorRequest(): RequestChannel.Request = {
    val findCoordinatorRequestData = new FindCoordinatorRequestData()
    val topicMetadataRequest = new FindCoordinatorRequest.Builder(findCoordinatorRequestData)
      .build(findCoordinatorRequestData.highestSupportedVersion())
    buildRequest(topicMetadataRequest)
  }

  private def buildCreatePartitionRequest(topicName: String, numPartitions: Int): RequestChannel.Request = {
    val createPartitionsTopic = new CreatePartitionsRequestData.CreatePartitionsTopic()
    createPartitionsTopic.setName(topicName)
    createPartitionsTopic.setCount(numPartitions)
    createPartitionsTopic.setAssignments(null)

    val createPartitionsRequestData = new CreatePartitionsRequestData()
    createPartitionsRequestData.setTopics(new CreatePartitionsTopicCollection(Collections.singletonList(createPartitionsTopic).iterator()))
    createPartitionsRequestData.setTimeoutMs(60000)
    val createPartitionsRequest = new CreatePartitionsRequest.Builder(createPartitionsRequestData)
      .build()

    buildRequest(createPartitionsRequest);
  }

  private def buildDeleteTopicsRequest(topicName: String): RequestChannel.Request = {
    val deleteTopicsRequestData = new DeleteTopicsRequestData()
      .setTopicNames(Collections.singletonList(topicName))
      .setTimeoutMs(60000)
    val deleteTopicsRequest = new DeleteTopicsRequest.Builder(deleteTopicsRequestData)
      .build(deleteTopicsRequestData.highestSupportedVersion)
    buildRequest(deleteTopicsRequest)
  }

  private def buildRequest[T <: AbstractRequest](request: AbstractRequest,
                                                 listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                                                 fromPrivilegedListener: Boolean = false): RequestChannel.Request = {

    val buffer = request.serializeWithHeader(new RequestHeader(request.apiKey, request.version, clientId, 0))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, fromPrivilegedListener)
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestChannelMetrics, envelope = None)
  }

  private def createBasicMetadataRequest(topics: Map[String, (Int, Uuid)]): UpdateMetadataRequest = {
    val replicas = List(0.asInstanceOf[Integer]).asJava

    def createPartitionState(topic: String)(partition: Int) = new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(partition)
      .setControllerEpoch(1)
      .setLeader(0)
      .setLeaderEpoch(1)
      .setReplicas(replicas)
      .setZkVersion(0)
      .setReplicas(replicas)

    val broker1 = updateMetadataBroker(1, "r1")
    val broker2 = updateMetadataBroker(2, "r2")
    val broker3 = updateMetadataBroker(3, "r3")
    val partitionStates = topics.flatMap { case (topic, (numPartitions, _)) =>
      (0 until numPartitions).map(createPartitionState(topic))
    }.toSeq
    new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, 0,
      0, 0, partitionStates.asJava, Seq(broker1, broker2, broker3).asJava,
      topics.map { case (topic, (_, uuid)) => topic -> uuid }.toMap.asJava).build()
  }

  private def setupBasicMetadataCache(topic: String, numPartitions: Int, uuid: Uuid): Unit = {
    val updateMetadataRequest = createBasicMetadataRequest(Map(topic -> (numPartitions, uuid)))
    metadataCache.updateMetadata(correlationId = 0, updateMetadataRequest)
  }

  private def setupBasicMetadataCache(topics: Map[String, (Int, Uuid)]): Unit = {
    val updateMetadataRequest = createBasicMetadataRequest(topics)
    metadataCache.updateMetadata(correlationId = 0, updateMetadataRequest)
  }

  private def createDeleteTopicMetadataRequest(topic: String, numPartitions: Int, uuid: Uuid): UpdateMetadataRequest = {
    val replicas = List(0.asInstanceOf[Integer]).asJava

    def createPartitionState(partition: Int) = new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(partition)
      .setControllerEpoch(1)
      .setLeader(LeaderAndIsr.LeaderDuringDelete)
      .setLeaderEpoch(1)
      .setReplicas(replicas)
      .setZkVersion(0)
      .setReplicas(replicas)

    val broker1 = updateMetadataBroker(1, "r1")
    val broker2 = updateMetadataBroker(2, "r2")
    val broker3 = updateMetadataBroker(3, "r3")
    val partitionStates = (0 until numPartitions).map(createPartitionState)
    new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion, 0,
      0, 0, partitionStates.asJava, Seq(broker1, broker2, broker3).asJava,
      Collections.singletonMap(topic, uuid)).build()
  }


  private def updateMetadataCacheWithDelete(topic: String, numPartitions: Int, uuid: Uuid): Unit = {
    val updateMetadataRequest = createDeleteTopicMetadataRequest(topic, numPartitions, uuid)
    metadataCache.updateMetadata(correlationId = 0, updateMetadataRequest)
  }

  private def updateMetadataBroker(id: Int, rack: String): UpdateMetadataBroker = {
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    new UpdateMetadataBroker()
      .setId(id)
      .setRack(rack)
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setListener(plaintextListener.value)).asJava)
  }
}
