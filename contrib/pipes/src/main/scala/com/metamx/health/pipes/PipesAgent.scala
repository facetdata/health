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

package com.metamx.health.pipes

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.event.Metric
import com.metamx.common.scala.event.WARN
import com.metamx.common.scala.net.curator.Discotheque
import com.metamx.common.scala.net.finagle.DiscoResolver
import com.metamx.common.scala.option._
import com.metamx.common.scala.time.Intervals
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.HeartbeatingAgent
import com.metamx.health.agent.PeriodicAgent
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.rainer.Commit
import com.metamx.rainer.CommitKeeper
import com.twitter.finagle.Group
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.http.Method.Get
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.Version.Http11
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Witness
import com.twitter.util.{Duration => TwitterDuration}
import java.util.concurrent.atomic.AtomicReference
import org.joda.time.DateTime
import org.joda.time.Duration

class PipesAgent(
  val emitter: ServiceEmitter,
  val pagerInventory: Option[NotifyInventory],
  discotheque: Discotheque,
  rainerKeeper: CommitKeeper[RainerPipes],
  config: HealthPipesConfig
) extends PeriodicAgent with HeartbeatingAgent with Logging
{
  implicit def jodaDurationToTwitterDuration(duration: Duration): TwitterDuration = {
    TwitterDuration.fromMilliseconds(duration.getMillis)
  }

  // Rainer key => Rainer config.
  private val rainers = new AtomicReference[Map[String, Commit[RainerPipes]]]()

  // Rainer watcher shutter-downer.
  private lazy val closer = rainerKeeper.mirror().changes.register(Witness(rainers))

  // Service discovery key => Client.
  private val clients = ConcurrentMap[String, Service[Request, Response]]()

  // (Rainer key, pipe id) => Last alert time (for throttling).
  private val lastAlert = ConcurrentMap[(String, String), DateTime]()

  // (Rainer key => (pipes job name => (Last checked failure time, Is pager triggered))) (for throttling and alerting only after retries).
  private val lastFailed = ConcurrentMap[String, Map[String, (DateTime, Boolean)]]()

  override def heartbeatPeriodMs = config.heartbeatPeriod.toStandardDuration.getMillis

  override def label = "pipes backlog"

  override def period = config.period

  override def init() {
    // Force initialization.
    closer
  }

  override def probe() = {
    dampedHeartbeat()

    val snapshot: Map[String, RainerPipes] = {
      for {
        (env, commit) <- rainers.get()
        alertConfig <- commit.value match {
          case Some(Right(x)) => Some(x)
          case None => None
          case Some(Left(e)) =>
            val msg = "Bad Pipes backlog alert config: %s" format env
            raiseAlert(e, Severity.ANOMALY, msg, Map.empty[String, Any])
            None
        }
      } yield {
        (env, alertConfig)
      }
    }

    // Close unnecessary clients.
    for (service <- clients.keySet if !snapshot.exists(_._2.control == service)) {
      log.info("Closing service: %s", service)
      val client = clients.remove(service).get
      Await.result(client.close())
    }

    // Probe things.
    val barriers = for ((env, alertConfig) <- snapshot) yield {
      // getOrElseUpdate is not atomic on ConcurrentMaps, but that's okay because only one of these agents will run
      // at any one time.
      val client = clients.getOrElseUpdate(alertConfig.control, {
        log.info("Creating service: %s", alertConfig.control)

        // TODO: Not sure if the Disco-based resolver is closed correctly.
        // TODO: May need to register DiscoResolver as an actual Resolver (in META-INF/services).
        // TODO: May need to use a Finagle that has a fix for https://github.com/twitter/finagle/issues/240.
        ClientBuilder()
          .name(alertConfig.control)
          .codec(Http())
          .group(Group.fromVarAddr(new DiscoResolver(discotheque.disco(alertConfig)).bind(alertConfig.control)))
          .hostConnectionLimit(2)
          .tcpConnectTimeout(config.httpTimeout)
          .timeout(config.httpTimeout)
          .daemon(true)
          .build()
      })
      val workRequest = Request(Http11, Get, "/zk/work") withEffect {
        req =>
          req.headerMap.set("Host", "pipes")
          req.headerMap.set("Accept", "*/*")
      }
      val pipelinesRequest = Request(Http11, Get, "/pipelines?full=true") withEffect {
        req =>
          req.headerMap.set("Host", "pipes")
          req.headerMap.set("Accept", "*/*")
      }

      val workResponse: Future[Map[String, Dict]] = client(workRequest) map {
        response =>
          if (response.statusCode / 200 != 1) {
            throw new IllegalStateException(
              "GET failed: %s %s" format
                (response.statusCode, response.status.reason)
            )
          } else {
            (Jackson.parse[Seq[_]](response.contentString) map {
              x =>
                val Seq(id, data) = list(x)
                (str(id), dict(data))
            }).toMap
          }
      } onFailure {
        e =>
          raiseAlert(e, WARN, "Failed to retrieve work queue: %s" format alertConfig.control, Map(
            "env" -> env,
            "controlService" -> alertConfig.control
          ))
      }

      val pipelinesResponse: Future[List[Dict]] = client(pipelinesRequest) map {
        response =>
          if (response.statusCode / 200 != 1) {
            throw new IllegalStateException(
              "GET failed: %s %s" format
                (response.statusCode, response.status.reason)
            )
          } else {
            Jackson.parse[Dict](response.contentString).map(kv => dict(kv._2)).toList
          }
      } onFailure {
        e =>
          raiseAlert(e, WARN, "Failed to retrieve pipelines: %s" format alertConfig.control, Map(
            "env" -> env,
            "controlService" -> alertConfig.control
          ))
      }

      Future.join(workResponse, pipelinesResponse) map {
        case (items, pipelines) =>
          // TODO: Report metrics, always.
          val now = DateTime.now

          // Compute channel for each pipe.
          val channelByPipe: Map[String, Option[String]] = {
            (pipelines flatMap (p => list(p("pipes"))) map (dict(_)) map {
              pipeDict =>
                (str(pipeDict("id")), Option(pipeDict.getOrElse("channel", null)).map(str(_)))
            }).toMap
          }

          // Compute backlog for each pipe.
          val minTimeByPipe: Map[String, DateTime] = {
            items.groupBy(_._2.get("pipeId")).strictMapValues(_.values) flatMap {
              case (pipeId, pipeItems) =>
                val intervals = Intervals(
                  pipeItems
                    .flatMap(d => list(d.getOrElse("intervals", Nil)))
                    .map(new Interval(_))
                )
                if (pipeId.isDefined && intervals.nonEmpty) {
                  log.debug(
                    "Pipe[%s] on control[%s] has work minTime[%s].",
                    pipeId.get, alertConfig.control, intervals.head.start
                  )
                  Some((str(pipeId.get), intervals.head.start))
                } else if (pipeId.isDefined) {
                  log.debug(
                    "Pipe[%s] on control[%s] has work items, but no actual queued work.",
                    pipeId.get, alertConfig.control
                  )
                  None
                } else {
                  None
                }
            }
          }

          // Compute failed jobs
          val failed = for ((job, info) <- items.filter { case (job, _) => job.contains(".failed")}) yield {
            if (info.contains("more") && dict(info("more")).contains("log")) {
              (job.replace(".failed", ""), (str(dict(info("more"))("log")), new DateTime()))
            } else {
              (job.replace(".failed", ""), ("No log found for the job", new DateTime()))
            }

          }

          // Resolve pager if needed
          lastFailed.getOrElse(env, Map()) filter { case (job, (seenTime, triggered)) =>
            triggered && !failed.contains(job)
          } foreach { case (job, _) =>
            alertConfig.failurePagerKeyOpt ifDefined { serviceKey =>
              resolvePager("pipes/failed/%s/%s".format(env, job), serviceKey)
            }
          }

          val lastChecked = lastFailed.getOrElse(env, Map())

          // Compute duration since last pager, trigger pager if needed, and create a map of failed tasks for env
          val failedPipes: Iterable[(String, (DateTime, Boolean))] = for ((job, (logUrl, seenTime)) <- failed) yield {
            log.info("pipe failed: " + job)
            lastChecked.get(job) match {
              case Some((lastTime, triggered)) =>
                val throttle = alertConfig.specificFailurePagerThrottles
                  .getOrElse(job, alertConfig.defaultFailurePagerThrottle).toStandardDuration.getMillis
                val currentTime = new DateTime()
                val duration = new Duration(lastTime, currentTime)
                val incidentKey = "pipes/failed/%s/%s".format(env, job)
                val description = "Pipe failed: %s".format(job)
                val details = Map(
                  "log" -> logUrl
                )

                // Don't want to trigger pager if throttle is 0
                if (throttle > 0L && (!triggered || duration.getMillis > throttle)) {
                  alertConfig.failurePagerKeyOpt ifDefined { serviceKey =>
                    triggerPager(incidentKey, serviceKey, description, details)
                  }
                  // Update seen time of the failed job for throttling
                  (job, (seenTime, true))
                } else {
                  (job, (lastTime, triggered))
                }

              case None =>
                (job, (seenTime, false))
            }
          }

          lastFailed.put(env, failedPipes.toMap)

          for ((pipeId, channel) <- channelByPipe) {
            val alertPeriod = alertConfig.specificDelayPagerPeriods.getOrElse(pipeId, {
              if (channel.isEmpty) alertConfig.defaultDelayPagerPeriod else new Period(0)
            })
            val alertThreshold = now - alertPeriod
            val minTime = minTimeByPipe.getOrElse(pipeId, now)
            val lagMillis = math.max(0, now.getMillis - minTime.getMillis)
            val lagString = "%.2f H" format (lagMillis.toDouble / 3600000.0)

            // Log.
            log.info("%s.%s.pipes.backlog = %s (channel = %s)", env, pipeId, lagString, channel.orNull)

            // Report metric.
            reportMetrics(
              Metric(
                "pipe/backlog",
                lagMillis / 1000,
                userDims = Map(
                  "pipesBacklogEnvironment" -> Seq(env),
                  "pipesBacklogControl" -> Seq(alertConfig.control),
                  "pipesBacklogPipeId" -> Seq(pipeId),
                  "pipesBacklogChannel" -> Seq(channel.getOrElse("null"))
                )
              )
            )

            // Raise pager, maybe. NB: Zero-length periods indicate disabled alerts.
            if (alertThreshold != now && minTime < alertThreshold) {
              val lastAlertAt = lastAlert.getOrElse((env, pipeId), new DateTime(0))
              if (now > lastAlertAt + config.alertThrottle) {
                alertConfig.delayPagerKeyOpt ifDefined { serviceKey =>
                  triggerPager(
                    "pipes/backlogged/%s/%s".format(env, pipeId),
                    serviceKey,
                    "Pipe backlogged: %s".format(pipeId),
                    Map(
                      "controlService" -> alertConfig.control,
                      "alertPeriod" -> alertPeriod.toString(),
                      "minTime" -> minTime.toString(),
                      "backlog" -> lagString,
                      "channel" -> channel.orNull
                    )
                  )
                }
                lastAlert((env, pipeId)) = now
              }
            } else if (minTime >= alertThreshold) { // Resolve pager if triggered
              lastAlert.get(env, pipeId) ifDefined { pagedTime =>
                alertConfig.delayPagerKeyOpt ifDefined { serviceKey =>
                  resolvePager("pipes/backlogged/%s/%s".format(env, pipeId), serviceKey)
                }
                lastAlert.remove((env, pipeId))
              }
            }
          }
      }
    }

    Await.result(Future.collect(barriers.toList))
    true
  }

  override def close() {
    Await.result(closer.close())
    Await.result(Future.collect(clients.values.map(_.close()).toList))
    rainers.set(null)
    clients.clear()
  }
}
