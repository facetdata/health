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

package com.metamx.health.druid

import com.github.nscala_time.time.Imports._
import com.google.common.io.ByteStreams
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.control._
import com.metamx.common.scala.event._
import com.metamx.common.scala.exception._
import com.metamx.common.scala.net.curator._
import com.metamx.common.scala.net.uri._
import com.metamx.common.scala.option._
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.HeartbeatingAgent
import com.metamx.health.agent.PeriodicAgent
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.rainer.Commit
import com.metamx.rainer.CommitKeeper
import com.twitter.util.Await
import com.twitter.util.Witness
import java.io.IOException
import java.util.concurrent.atomic.AtomicReference
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import scala.collection.mutable

/**
 * Probes Druid maxTimes and replication status for all druid data sources.
 *
 * TODO- Switch from Apache HTTP to Finagle.
 */
class DruidAgent(
  val emitter: ServiceEmitter,
  val pagerInventory: Option[NotifyInventory],
  discotheque: Discotheque,
  rainerKeeper: CommitKeeper[RainerDruid],
  config: HealthDruidConfig
) extends PeriodicAgent with HeartbeatingAgent with Logging
{
  case class DataSource(environment: String, dataSource: String)

  private val httpClient = Http.create(10, config.httpTimeout)

  private val allDruids = new AtomicReference[Map[Commit.Key, Commit[RainerDruid]]]

  private val isCurrentlyFailing       = mutable.Map[DataSource, Boolean]()
  private val lastStaleDataAlert       = mutable.Map[DataSource, DateTime]()
  private val nonZeroLoadStatusStarted = mutable.Map[DataSource, DateTime]()

  private lazy val closer = rainerKeeper.mirror().changes.register(Witness(allDruids))

  override def heartbeatPeriodMs = config.heartbeatPeriod.toStandardDuration.getMillis

  override def period = config.period

  override def label = "druid cluster"

  override def init() {
    // Force initialization.
    closer
  }

  def http(request: HttpUriRequest): HttpResponse = {
    retryOnErrors(ifException[IOException] untilCount 3) {
      httpClient.execute(request)
    }
  }

  // Given a broker URI, find datasources it is aware of.
  private def brokerDataSources(uri: URI): Seq[String] = {
    val response = http(new HttpGet(uri.withPath(_.stripSuffix("/") + "/druid/v2/datasources")))
    if (response.getStatusLine.getStatusCode / 200 != 1) {
      throw new IllegalStateException(
        "Bad status from broker: %s %s" format
          (response.getStatusLine.getStatusCode, response.getStatusLine.getReasonPhrase)
      )
    }
    Jackson.parse[Seq[String]](ByteStreams.toByteArray(response.getEntity.getContent))
  }

  // Given a coordinator URI, find datasources it is aware of, and their load statuses.
  private def coordinatorLoadStatuses(uri: URI): Map[String, Int] = {
    val response = http(
      new HttpGet(uri.withPath(_.stripSuffix("/") + "/druid/coordinator/v1/loadstatus").withQuery("simple"))
    )
    if (response.getStatusLine.getStatusCode / 200 != 1) {
      throw new IllegalStateException(
        "Bad status from coordinator: %s %s" format
          (response.getStatusLine.getStatusCode, response.getStatusLine.getReasonPhrase)
      )
    }
    Jackson.parse[Dict](ByteStreams.toByteArray(response.getEntity.getContent)).strictMapValues(int(_))
  }

  override def probe() = {
    dampedHeartbeat()

    val allDruidsSnapshot: Map[String, RainerDruid] = {
      for {
        (environmentName, commit) <- allDruids.get()
        commitValue <- commit.value match {
          case Some(Right(x)) => Some(x)
          case None => None
          case Some(Left(e)) =>
            val msg = "Bad config for druid environment: %s" format environmentName
            raiseAlert(e, Severity.ANOMALY, msg, Map.empty[String, Any])
            None
        }
      } yield {
        (environmentName, commitValue)
      }
    }

    // Possibly raise load status alerts for each dataSource that the coordinator is aware of.
    for {
      (env, rainerConfig@RainerDruid(_, _, _, coordinatorService, _)) <- allDruidsSnapshot
      coordinatorUri <- discotheque.disco(rainerConfig).instanceFor(coordinatorService).map(_.uri).ifEmpty {
        raiseAlert(WARN, "Failed to locate Druid coordinator: %s" format coordinatorService, Map())
      }
      tuples <- coordinatorLoadStatuses(coordinatorUri).toList swallow {
        case e: Exception =>
          raiseAlert(
            e, WARN, "Failed to fetch Druid loadStatuses: %s" format coordinatorService, Map(
              "druid coordinator uri" -> coordinatorUri
            )
          )
      }
      (dataSource, loadStatus) <- tuples
    } {
      val now = DateTime.now
      log.info("%s.%s.loadstatus = %s", env, dataSource, loadStatus)
      if (loadStatus > 0) {
        // Still have segments left to load.
        val startedAt = nonZeroLoadStatusStarted.getOrElseUpdate(DataSource(env, dataSource), now)
        if (startedAt + config.loadStatusGracePeriod < now) {
          raiseAlert(
            ERROR, "Data not loaded: %s" format dataSource, Map(
              "druid environment" -> env,
              "druid dataSource" -> dataSource,
              "druid coordinator service" -> coordinatorService,
              "druid coordinator uri" -> coordinatorUri,
              "loadStatus" -> loadStatus,
              "started at" -> startedAt.toString()
            )
          )
        }
      } else {
        nonZeroLoadStatusStarted.remove(DataSource(env, dataSource))
      }
    }

    // Report metrics, and possibly raise alerts, for maxTimes for each dataSource that the broker is aware of.
    for {
      (env, rainerConfig@RainerDruid(_, _, brokerService, _, alerts)) <- allDruidsSnapshot
      brokerUri <- discotheque.disco(rainerConfig).instanceFor(brokerService).map(_.uri).ifEmpty {
        raiseAlert(WARN, "Failed to locate Druid broker: %s" format brokerService, Map())
      }
      dataSources <- brokerDataSources(brokerUri) swallow {
        case e: Exception =>
          raiseAlert(
            e, WARN, "Failed to fetch Druid dataSources: %s" format brokerService, Map(
              "druid broker uri" -> brokerUri
            )
          )
      }
      dataSource <- dataSources
    } {
      val druidDetails = Map(
        "druid environment" -> env,
        "druid dataSource" -> dataSource,
        "druid broker service" -> brokerService,
        "druid broker uri" -> brokerUri
      )
      val maxTime = try {
        val maxTimeRequest = new HttpPost(brokerUri.withPath(_.stripSuffix("/") + "/druid/v2/"))
        maxTimeRequest.setEntity(
          new ByteArrayEntity(
            Jackson.bytes(Map("queryType" -> "maxTime", "dataSource" -> dataSource)),
            ContentType.APPLICATION_JSON
          )
        )
        val maxTimeResponse = http(maxTimeRequest)
        val maxTimeResponseBytes = ByteStreams.toByteArray(maxTimeResponse.getEntity.getContent)
        try {
          val maxTimeSeq = Jackson.parse[Seq[Any]](maxTimeResponseBytes)
          Some(new DateTime(dict(maxTimeSeq.head)("result")))
        }
        catch {
          case e: Exception =>
            throw new IllegalArgumentException(
              "Cannot parse response from broker: %s" format
                new String(maxTimeResponseBytes)
            )
        }
      }
      catch {
        case e: Exception =>
          val message = "Failed to get maxTime for dataSource: %s, broker: %s" format(dataSource, brokerService)
          raiseAlert(e, WARN, message, druidDetails)
          None
      }
      maxTime foreach {
        mt =>
          val now = DateTime.now
          val lagMillis = now.getMillis - mt.getMillis
          val lagString = "%.2f H" format (lagMillis.toDouble / 3600000.0)
          log.info("%s.%s.datalag = %s", env, dataSource, lagString)

          reportMetrics(
            Metric(
              "maxtime/lag",
              lagMillis / 1000,
              userDims = Map(
                "druidEnvironment" -> Seq(env),
                "druidDataSource" -> Seq(dataSource),
                "druidBrokerService" -> Seq(brokerService)
              )
            )
          )

          // Trigger or resolve pager, maybe.
          alerts.get(dataSource) foreach {
            maxPeriod =>
              alerts.get(dataSource) match {
                case None => // No alerts configured for this datasource
                case Some(alertPayload) =>
                  val dataSourceKey = DataSource(env, dataSource)
                  val incidentKey = "druid/staleData/%s/%s" format (env, dataSource) // PagerDuty incident key
                  val description = "Stale data: %s" format dataSource // Email subject

                  if (now - alertPayload.threshold > mt) {
                    val last = lastStaleDataAlert.getOrElse(dataSourceKey, new DateTime(0))
                    if (now > last + config.staleDataAlertThrottle) {
                      log.info("Stale data triggered for dataSource[%s] in environment[%s].", dataSource, env)

                      isCurrentlyFailing(dataSourceKey) = true
                      lastStaleDataAlert(dataSourceKey) = now

                      // Build alert for possible triggering or resolving pager.
                      val details = Map(
                        "maxtime lag" -> lagString,
                        "threshold" -> maxPeriod.toString
                      ) ++ druidDetails

                      raiseAlert(ERROR, description, details)
                      alertPayload.pagerKey ifDefined { pagerKey =>
                        triggerPager(incidentKey, pagerKey, description, details)
                      }
                    }
                  } else if (isCurrentlyFailing.getOrElse(dataSourceKey, false)) {
                    // Everything is OK again. Resolve alerts.
                    log.info("Stale data resolved for dataSource[%s] in environment[%s].", dataSource, env)
                    alertPayload.pagerKey ifDefined { pagerKey =>
                      resolvePager(incidentKey, pagerKey)
                    }
                    isCurrentlyFailing(dataSourceKey) = false
                  }
              }
          }
      }
    }
    true
  }

  override def close() {
    Await.result(closer.close())
  }
}
