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

package com.metamx.health.hq.api

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.metamx.health.hq.db.{PostgreSQLDB, PostgreSqlStatusStorageMixin}
import org.scalatra.ScalatraServlet

class StatusServlet(db: PostgreSQLDB with PostgreSqlStatusStorageMixin) extends ScalatraServlet
{
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(new JodaModule())
  mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

  before() {
    contentType = "application/json"
  }

  get("/") {
    mapper.writeValueAsString(Map("statuses" -> db.allStatuses()))
  }

  get("/tags/:tag/:value") {
    val tag = params("tag")
    val value = params("value")
    mapper.writeValueAsString(Map("statuses" -> db.statusByTag(tag, value)))
  }

  get("/history/:historySize/*") {
    val track = multiParams("splat").head.split("/")
    val size = params("historySize")
    mapper.writeValueAsString(Map("statuses" -> db.statusHistoryByTrack(track, size.toInt)))
  }

  get("/track/*") {
    val track = multiParams("splat").head.split("/")
    mapper.writeValueAsString(Map("statuses" -> db.statusByTrack(track).headOption.orNull))
  }

  get("/tracks") {
    mapper.writeValueAsString(db.allTracks)
  }
}
