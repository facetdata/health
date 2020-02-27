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

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonRawValue
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.collect.ImmutableMap
import com.metamx.common.scala.untyped.Dict
import com.metamx.emitter.service.ServiceEventBuilder
import org.joda.time.DateTime

@JsonIgnoreProperties(ignoreUnknown = true)
case class PagerEvent(
  timestamp: String,
  service: String,
  host: String,
  page: Boolean,
  @JsonRawValue data: JsonNode
) extends ServiceEventAdapter
{
  def safeToBuffer = false
  def feed = "pagers"

  override def toDict: Dict = {
    Map(
      "timestamp" -> timestamp,
      "service" -> service,
      "host" -> host,
      "page" -> page,
      "data" -> data
    )
  }
}

class PagerEventBuilder(
  page: Boolean,
  data: JsonNode
) extends ServiceEventBuilder[PagerEvent]
{
  override def build(serviceDims: ImmutableMap[String, String]): PagerEvent = {
    PagerEvent(
      timestamp = new DateTime().toString,
      service = serviceDims.get("service"),
      host = serviceDims.get("host"),
      page = page,
      data = data
    )
  }
}

object PagerData
{
  private val DefaultMapper = new ObjectMapper()
  DefaultMapper.registerModule(DefaultScalaModule)
  DefaultMapper.registerModule(new JodaModule())

  def toRaw(data: PagerData): JsonNode = toRaw(data, DefaultMapper)

  def fromRaw(node: JsonNode): PagerData = fromRaw(node, DefaultMapper)

  def toRaw(data: PagerData, mapper: ObjectMapper): JsonNode = {
    mapper.valueToTree(data)
  }

  def fromRaw(node: JsonNode, mapper: ObjectMapper): PagerData = {
    mapper.treeToValue(node, classOf[PagerData])
  }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(Array(
  new Type(name = "trigger", value = classOf[PagerTriggerData]),
  new Type(name = "resolve", value = classOf[PagerResolveData])
))
sealed trait PagerData

case class PagerTriggerData(
  incidentKey: String,
  serviceKey: String,
  description: String,
  details: Dict
) extends PagerData

case class PagerResolveData(
  incidentKey: String,
  serviceKey: String
) extends PagerData
