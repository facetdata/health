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

package com.metamx.health.agent.events

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.google.common.collect.ImmutableMap
import com.metamx.common.scala.untyped.Dict
import com.metamx.emitter.service.ServiceEventBuilder
import org.joda.time.DateTime

@JsonIgnoreProperties(ignoreUnknown = true)
case class StatusEvent(
  timestamp: String,
  service: String,
  host: String,
  agent: String,
  track: IndexedSeq[String],
  tags: Map[String, String],
  value: String,
  message: String
) extends ServiceEventAdapter
{
  def safeToBuffer = false
  def feed = "statuses"

  override def toDict: Dict = {
    Map(
      "timestamp" -> timestamp,
      "service" -> service,
      "host" -> host,
      "agent" -> agent,
      "track" -> track,
      "tags" -> tags,
      "value" -> value,
      "message" -> message
    )
  }
}

class StatusEventBuilder(
  agent: String,
  track: IndexedSeq[String],
  tags: Map[String, String],
  value: String,
  message: String
) extends ServiceEventBuilder[StatusEvent]
{
  override def build(serviceDims: ImmutableMap[String, String]): StatusEvent = {
    StatusEvent(
      timestamp = new DateTime().toString,
      service = serviceDims.get("service"),
      host = serviceDims.get("host"),
      agent = agent,
      track = track,
      tags = tags,
      value = value,
      message = message
    )
  }
}
