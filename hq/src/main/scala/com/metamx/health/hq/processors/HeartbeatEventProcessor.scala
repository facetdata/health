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

import com.google.common.base.Throwables
import com.metamx.common.scala.collection.mutable._
import com.metamx.common.scala.concurrent.locks.LockOps
import com.metamx.common.scala.event.{Metric, emit}
import com.metamx.common.scala.untyped._
import com.metamx.common.scala.{Logging, Yaml}
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.events.HeartbeatEvent
import com.metamx.health.hq.config.HealthHeartbeatConfig
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.health.rainer.RainerUtils
import com.metamx.health.rainer.RainerUtils.ConfigData
import com.metamx.rainer.{Commit, CommitKeeper, KeyValueDeserialization}
import com.twitter.util.Witness
import java.io.ByteArrayInputStream
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.joda.time.{DateTime, Period}
import com.github.nscala_time.time.Imports._
import scala.collection.mutable

case class HeartbeatConfig(
  agents: Seq[String],
  period: Period
)

class HeartbeatEventProcessor(
  config: HealthHeartbeatConfig,
  notifyInventory: NotifyInventory,
  rainerKeeper: CommitKeeper[HeartbeatConfig],
  emitter: ServiceEmitter,
  topic: String,
  kafkaProps: Properties
) extends EventProcessor[HeartbeatEvent](emitter, topic, kafkaProps)
{
  case class Heartbeat(
    timestamp: DateTime,
    service: String,
    host: String
  )

  class HeartbeatEventProcessorState(
    val config: HeartbeatConfig,
    val agents: ConcurrentMap[String, Heartbeat]
  )

  // read lock - for config users, write lock - for config updates
  private val lock = new ReentrantReadWriteLock()

  private val unexpectedAgentAlertTimes = new mutable.HashMap[String, DateTime]()
  private val downAgentAlertFlags = new mutable.HashSet[String]()

  private val initialized = new AtomicBoolean(false)
  @volatile private var stateOpt: Option[HeartbeatEventProcessorState] = None

  private def withState[A](f: (HeartbeatEventProcessorState) => A): A = {
    initialized.synchronized {
      if (!initialized.get()) {
        log.info("Waiting for heartbeats watchdog config initialization")

        while (!initialized.get()) {
          try {
            initialized.wait()
          } catch {
            case e: InterruptedException => // NOP
          }
        }
      }
    }


    val state = stateOpt getOrElse {
      throw new IllegalStateException("heartbeats watchdog config is not initialized")
    }
    f(state)
  }
  
  private lazy val closer = rainerKeeper.mirror().changes.register(
    new Witness[Map[Commit.Key, Commit[HeartbeatConfig]]]
    {
      override def notify(note: Map[Commit.Key, Commit[HeartbeatConfig]]) {
        lock.writeLock() {
          log.info("Reinitializing monitored agents list – received configs update notification")
          try {
            val configData = RainerUtils.configByKey(config.rainerConfigKey, note)
            configData match {
              case Some(ConfigData(newConfig, newConfigStr)) =>
                log.info("Inventory config for key '%s':\n%s\n\n".format(config.rainerConfigKey, newConfigStr))

                try {
                  stateOpt match {
                    case Some(state) if state.config == newConfig =>
                      log.info("Doing nothing - new and old configs are identical")

                    case _ =>
                      stateOpt = Some(new HeartbeatEventProcessorState(
                        newConfig,
                        ConcurrentMap[String, Heartbeat](
                          newConfig.agents.map(x => (x, new Heartbeat(timestamp = new DateTime(), service = "n/a", host = "n/a"))):_*
                        )
                      ))
                  }
                } finally {
                  initialized.synchronized {
                    initialized.set(true)
                    initialized.notifyAll()
                  }
                }

              case None =>
                val description = "Unable to reinitialize monitored agents list - missing config"
                val details = Map(
                  "rainerConfigKey" -> config.rainerConfigKey
                )
                emit.emitAlert(log, emitter, Severity.SERVICE_FAILURE, description, details)
                notifyInventory withPagerDuty {
                  pd =>
                    pd.trigger(HeartbeatEventProcessor.pagerWatchdogConfigFailedKey, config.pagerKey, description, details)
                }
            }
          } catch {
            case e: Throwable =>
              val description = "Unable to reinitialize monitored agents list - exception"
              val details = Map(
                "exceptionType" -> e.getClass.getName,
                "exceptionMessage" -> e.getMessage,
                "exceptionStackTrace" -> Throwables.getStackTraceAsString(e)
              )
              emit.emitAlert(e, log, emitter, Severity.SERVICE_FAILURE, description, details)
              notifyInventory withPagerDuty {
                pd =>
                  pd.trigger(HeartbeatEventProcessor.pagerWatchdogConfigFailedKey, config.pagerKey, description, details)
              }
          }
          log.info("Monitored agents list reinitialized")
        }
      }
    }
  )

  def init() {
    // Force initialization
    closer

    // Waiting for the first rainer config update
    withState { _ => }

    val watchdogThread = new Thread() {
      override def run() {
        try {
          while (true) {
            lock.readLock() {
              withState {
                state =>
                  for ((agent, Heartbeat(timestamp, service, host)) <- state.agents) {
                    val currentTime = new DateTime()

                    val maxDelayMs = state.config.period.toStandardDuration.getMillis
                    val heartbeatDelayMs = currentTime.getMillis - timestamp.getMillis
                    if (heartbeatDelayMs > maxDelayMs) {
                      if (!downAgentAlertFlags.contains(agent)) {
                        downAgentAlertFlags.add(agent)

                        val description = "Agent [%s] is down".format(agent)
                        val details = Map(
                          "last heartbeat time" -> timestamp.toString(),
                          "current time" -> currentTime.toString(),
                          "delay (sec)" -> "%,d".format(heartbeatDelayMs / 1000),
                          "max delay (sec)" -> "%,d".format(maxDelayMs / 1000)
                        )
                        emit.emitAlert(log, emitter, Severity.COMPONENT_FAILURE, description, details)
                        notifyInventory withPagerDuty {
                          pd =>
                            pd.trigger(HeartbeatEventProcessor.pagerAgentDownKey(agent), config.pagerKey, description, details)
                        }
                      }
                    } else {
                      if (downAgentAlertFlags.contains(agent)) {
                        downAgentAlertFlags.remove(agent)

                        log.info("Agent %s has recovered".format(agent))
                        notifyInventory withPagerDuty {
                          pd =>
                            pd.resolve(HeartbeatEventProcessor.pagerAgentDownKey(agent), config.pagerKey)
                        }
                      }
                    }
                  }
              }
            }
            Thread.sleep(config.watchdogPeriod.toStandardDuration.getMillis)
          }
        } catch {
          case e: Throwable =>
            val description = "Health heartbeats watchdog thread failed"
            val details = Map(
              "exceptionType" -> e.getClass.getName,
              "exceptionMessage" -> e.getMessage,
              "exceptionStackTrace" -> Throwables.getStackTraceAsString(e)
            )

            emit.emitAlert(e, log, emitter, Severity.SERVICE_FAILURE, description, details)
            notifyInventory withPagerDuty {
              pd =>
                pd.trigger(HeartbeatEventProcessor.pagerWatchdogThreadFailedKey, config.pagerKey, description, details)
            }
        }
      }
    }
    watchdogThread.setDaemon(true)
    watchdogThread.start()
  }

  override def process(event: HeartbeatEvent) {
    lock.readLock() {
      log.info("Received heartbeat from %s: [%s] %s %s".format(
        event.agent, event.timestamp, event.service, event.host
      ))

      withState {
        state =>
          if (state.agents.contains(event.agent)) {
            state.agents.put(event.agent, Heartbeat(new DateTime(), event.service, event.host))
          } else {
            unexpectedAgentAlertTimes.synchronized {
              val nowTime = DateTime.now
              unexpectedAgentAlertTimes.get(event.agent) match {
                case Some(lastPagerTime) if lastPagerTime + config.unexpectedAgentAlertPeriod > nowTime =>
                  // Do nothing, too little time has passed since the last pager

                case _ =>

                  val description = "Unexpected heartbeat from agent <%s>".format(event.agent)
                  val details = Map(
                    "heartbeatTimestamp" -> event.timestamp,
                    "heartbeatService" -> event.service,
                    "heartbeatHost" -> event.host,
                    "heartbeatAgent" -> event.agent
                  )

                  alertAggregator.put(description, details)
                  notifyInventory withPagerDuty {
                    pd =>
                      pd.trigger(HeartbeatEventProcessor.pagerAgentUnexpected(event.agent), config.pagerKey, description, details)
                  }
              }
              unexpectedAgentAlertTimes.put(event.agent, nowTime)
            }
          }
      }
    }
  }

  override def metrics(event: HeartbeatEvent): Option[Metric] = {
    val dimensions = Map(
      "service" -> Seq(event.service),
      "host" -> Seq(event.host),
      "agent" -> Seq(event.agent)
    )
    Some(Metric("heartbeat", 1, userDims = dimensions, created = event.timestampTT))
  }
}

object HeartbeatEventProcessor extends Logging
{
  def pagerWatchdogConfigFailedKey = "health/heartbeat/watchdog/config-failed"
  def pagerWatchdogThreadFailedKey = "health/heartbeat/watchdog/thread-failed"
  def pagerAgentDownKey(agent: String) = "health/agent/down/%s".format(agent)
  def pagerAgentUnexpected(agent: String) = "health/agent/unexpected/%s".format(agent)

  implicit val deserialization = new KeyValueDeserialization[HeartbeatConfig] {
    override def fromKeyAndBytes(k: Commit.Key, bytes: Array[Byte]) = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))
      val configDict = dict(d("config"))

      HeartbeatConfig(
        agents = list(configDict("agents")).map(str(_)),
        period = Period.parse(str(configDict("period")))
      )
    }
  }
}
