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

package com.metamx.health.hq.db

import com.fasterxml.jackson.databind.{ObjectMapper}
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.metamx.common.scala.db.DB
import com.metamx.common.scala.untyped._
import com.metamx.health.agent.events.StatusEvent
import java.sql.Timestamp
import org.joda.time.DateTime
import org.postgresql.jdbc42.Jdbc42Array
import org.postgresql.util.PGobject
import com.metamx.common.scala.option._
import org.apache.commons.lang.StringEscapeUtils


trait PostgreSqlStatusStorageMixin extends DbStatusStorageMixin
{
  self: DB =>

  override def statusTableSchema = Seq(
    "\"timestamp\"    timestamp with time zone NOT NULL",
    "service          character varying NOT NULL",
    "host             character varying NOT NULL",
    "agent            character varying NOT NULL",
    "track            character varying[] NOT NULL",
    "tags             jsonb NOT NULL",
    "value            character varying NOT NULL",
    "message          character varying NOT NULL",
    "CONSTRAINT status_pkey PRIMARY KEY (track)"
  )

  override def statusHistoryTableSchema = Seq(
    "id               serial primary key",
    "\"timestamp\"    timestamp with time zone NOT NULL",
    "service          character varying NOT NULL",
    "host             character varying NOT NULL",
    "agent            character varying NOT NULL",
    "track            character varying[] NOT NULL",
    "tags             jsonb NOT NULL",
    "value            character varying NOT NULL",
    "message          character varying NOT NULL"
  )


  var mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(new JodaModule())

  def timeFromDb(raw: Any): DateTime = {
    raw match {
      case tstamp: Timestamp => new DateTime(tstamp.getTime)
      case ts => throw new IllegalStateException("Unexpected 'timestamp' field type: %s".format(ts.getClass.getCanonicalName))
    }
  }

  def tagsToDb(tags: Map[String,String]): PGobject = {
    val tagsObject = new PGobject()
    tagsObject.setType("jsonb")
    tagsObject.setValue(mapper.writeValueAsString(tags))
    tagsObject
  }

  def tagsFromDb(tags: Any): Map[String,String] = {
    mapper.readValue(tags.toString, classOf[Map[String, String]])
  }

  def trackToDb(track: Seq[String]): PGobject = {
    val trackstr = "{%s}".format(track.map((t) => "\"%s\"".format(t)).mkString(","))
    val arrayObject = new PGobject()
    arrayObject.setType("varchar[]")
    arrayObject.setValue(trackstr)
    arrayObject
  }

  def trackfromDb(raw: Any): Seq[String] = {
    raw match {
      case array: Jdbc42Array => array.getArray() match {
        case a: Array[String] => a.toSeq
        case arr => throw new IllegalStateException("Unexpected 'track' field type: %s".format(arr.getClass.getCanonicalName))
      }
      case arr => throw new IllegalStateException("Unexpected 'track' field type: %s".format(arr.getClass.getCanonicalName))
    }
  }

  def buildStatus(row : Dict): Status = {
    val service = row("service").toString
    val host = row("host").toString
    val agent = row("agent").toString
    val value = row("value").toString
    val timestamp= timeFromDb(row("timestamp"))
    val track = trackfromDb(row("track"))
    val tags = tagsFromDb(row("tags"))

    val message = row("message").toString

    new Status(timestamp, service, host, agent, track, tags, value, message)
  }


  override def init(): Unit = {
    if (!schema.exists("status")) {
      log.info("Creating table[%s].", "status")
      createTable("status", statusTableSchema)
      createTable("status_history", statusHistoryTableSchema)

      execute("CREATE INDEX track_idx ON status_history (track);")
    }
  }

  override def insertOrUpdate(event: StatusEvent, historySize: Int): Unit = {
    inTransaction {
      val track = trackToDb(event.track)
      val tags = tagsToDb(event.tags)

      val sql =
        """
          |select status.timestamp
          |from status
          |where status.track = ?
        """.stripMargin

      val matches = select(sql, track)
        .map(row => timeFromDb(row("timestamp")))

      val timestamp = new Timestamp(event.timestampTT.getMillis)

      // don't update current status if we get an older event
      matches.headOption match {
        case Some(head) =>
          if (head.isBefore(event.timestampTT.getMillis)) {
            execute(
              """
                |update status set (timestamp, service, host, agent, tags, value, message) = (?,?,?,?,?,?,?)
                |where track = ?
              """.stripMargin,
              timestamp,
              event.service,
              event.host,
              event.agent,
              tags,
              event.value,
              event.message,
              track
            )
          }
        case None =>
          execute(
            "insert into status (timestamp, service, host, agent, track, tags, value, message) values (?,?,?,?,?,?,?,?)",
            timestamp,
            event.service,
            event.host,
            event.agent,
            track,
            tags,
            event.value,
            event.message
          )
      }

      // always update history
      execute(
        "insert into status_history (timestamp, service, host, agent, track, tags, value, message) values (?,?,?,?,?,?,?,?)",
        timestamp,
        event.service,
        event.host,
        event.agent,
        track,
        tags,
        event.value,
        event.message
      )

      // clean up old history
      // select history size, and delete anything older than the last one
      val historySql =
        """
          |select status_history.timestamp
          |from status_history
          |where status_history.track = ?
          |order by status_history.timestamp desc
          |limit 1 offset ?
        """.stripMargin

      val historyMatches = select(historySql, track, historySize - 1)
        .map(row => timeFromDb(row("timestamp")))

      historyMatches.lastOption ifDefined {
        last =>
          val cleanHistorySql =
            """
              |delete from status_history
              |where status_history.track = ? and status_history.timestamp < ?
            """.stripMargin
          execute(cleanHistorySql, track, last)
      }
    }
  }

  override def allStatuses(): Seq[Status] = {
    val sql =
      """
        |select timestamp, service, host, agent, track, tags, value, message
        |from status
      """.stripMargin

    select(sql).map(buildStatus)
  }

  override def statusByTrack(track: Seq[String]): Seq[Status] = {
    val _track = trackToDb(track)
    val sql =
      """
        |select timestamp, service, host, agent, track, tags, value, message
        |from status
        |where track @> ?
        |and track[1] = ?
      """.stripMargin

    select(sql, _track, track.head).map(buildStatus)
  }

  override def statusByTag(tag: String, value: String): Seq[Status] = {

    val sql =
      """
      |select timestamp, service, host, agent, track, tags, value, message
      |from status
      |where tags #>> %s = ?;
    """.
        format(StringEscapeUtils.escapeSql(tag)).stripMargin

    select(sql, value).map(buildStatus)
  }

  override def statusHistoryByTrack(track: Seq[String], historySize: Int): Seq[Status] = {
    val _track = trackToDb(track)
    val sql =
      """
        |select id, timestamp, service, host, agent, track, tags, value, message
        |from status_history
        |where track = ?
        |limit ?
      """.stripMargin

    select(sql, _track, historySize).map(buildStatus)
  }

  override def allTracks(): Seq[Seq[String]] = {
    val sql =
      """
        |select track
        |from status
      """.stripMargin

    select(sql).map((row) => trackfromDb(row("track"))).sortWith((s1,s2) => s1.mkString("/") < s2.mkString("/"))
  }
}
