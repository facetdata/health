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

package com.metamx.health.hq.processors

import com.metamx.emitter.service.ServiceEmitter
import com.metamx.common.scala.event.Metric
import com.metamx.health.agent.events.StatusEvent
import java.util.Properties
import com.metamx.health.hq.db.{PostgreSqlStatusStorageMixin, PostgreSQLDB}


class StatusEventProcessor(emitter: ServiceEmitter, topic: String, kafkaProps: Properties, historySize : Int, db: PostgreSQLDB with PostgreSqlStatusStorageMixin)
  extends EventProcessor[StatusEvent](emitter, topic, kafkaProps)
{
  override def process(event: StatusEvent) {
    db.insertOrUpdate(event, historySize)
  }

  override def metrics(event: StatusEvent): Option[Metric] = {
    val dimensions =
      event.tags.map{ case(k, v) => ("tags_" + k) ->  Seq(v) } ++
      event.track.zipWithIndex.map{ case(el, i) => ("track_" + i) ->  Seq(el) }.toMap +
      ("service" -> Seq(event.service)) +
      ("host" -> Seq(event.host)) +
      ("agent" -> Seq(event.agent)) +
      ("message" -> Seq(event.agent)) +
      ("status_value" -> Seq(event.value))


    Some(Metric("status", 1, userDims = dimensions, created = event.timestampTT))
  }

  def init() {
    db.init()
  }
}

