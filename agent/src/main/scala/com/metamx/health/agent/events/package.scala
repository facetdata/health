/*
 * Licensed to Facet Data, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Facet Data, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.health.agent

import com.fasterxml.jackson.annotation.JsonValue
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEvent
import org.joda.time.DateTime
import scala.collection.JavaConverters._

package object events
{
  trait ServiceEventAdapter extends ServiceEvent
  {
    def timestamp: String
    def service: String
    def host: String
    def safeToBuffer: Boolean
    def feed: String
    def toDict: Dict

    def timestampTT: DateTime = new DateTime(timestamp)

    override def getCreatedTime = timestampTT
    override def getService = service
    override def getHost = host
    override def isSafeToBuffer = safeToBuffer
    override def getFeed = feed

    @JsonValue override def toMap =
      (Map("feed" -> feed) ++ toDict.mapValues(x => x.asInstanceOf[AnyRef])).asJava
  }
}
