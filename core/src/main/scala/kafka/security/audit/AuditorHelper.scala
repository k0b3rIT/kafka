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

package kafka.security.audit

import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.server.auditor.Auditor.AuditInformation
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}

import scala.collection.{Map, Seq}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.server.auditor.{AuditEvent, Auditor}

import scala.jdk.CollectionConverters._

class AuditorHelper(auditors: List[Auditor]) {

  def audit(event: AuditEvent,
            ctx: RequestContext,
            operation: AclOperation,
            resourceType: ResourceType,
            resourceName: String,
            error: Errors): Unit = {

    val resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL)
    audit(event, ctx, Map(operation -> List(new AuditInformation(resource, error.code()))))
  }

  def audit(event: AuditEvent,
            ctx: RequestContext,
            authorizationInformation: Map[AclOperation, Seq[AuditInformation]]): Unit = {
    val authInfo = authorizationInformation.map { case (k, v) =>
      k -> v.toSet.asJava
    }.asJava
    auditors.foreach(auditor =>
      auditor.audit(event, ctx, authInfo))
  }
}
