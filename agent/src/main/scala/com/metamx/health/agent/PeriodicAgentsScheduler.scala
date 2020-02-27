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

import com.github.nscala_time.time.Imports._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.lifecycle.LifecycleStart
import com.metamx.common.lifecycle.LifecycleStop
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.concurrent.abortingRunnable
import com.metamx.common.scala.concurrent.callable
import com.metamx.common.scala.event.WARN
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.notify.inventory.NotifyInventory
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Schedules the execution of our agents.
 */
class PeriodicAgentsScheduler(
  val emitter: ServiceEmitter,
  val pagerKey: String,
  val pagerInventory: Option[NotifyInventory],
  agents: List[PeriodicAgent]
) extends Agent
{
  val label = "periodic agents scheduler"

  private[this] var started = true
  private[this] val exec    = Executors.newScheduledThreadPool(
    agents.size, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Agents-%d").build()
  )
  private[this] val agentStartTime = {
    new ArrayBuffer[DateTime]() with mutable.SynchronizedBuffer[DateTime] withEffect {
      list =>
        val now = DateTime.now
        for ((agent, idx) <- agents.zipWithIndex) {
          list += now
        }
    }
  }

  private[this] val hasPagerTriggered = mutable.Set[String]()

  private def internalTriggerPager(incidentKey: String, description: String, details: Dict) {
    triggerPager(incidentKey, pagerKey, description, details)
    hasPagerTriggered.synchronized { hasPagerTriggered.add(incidentKey) }
  }

  private def internalResolvePager(incidentKey: String) {
    if (hasPagerTriggered.synchronized { hasPagerTriggered.remove(incidentKey) }) {
      resolvePager(incidentKey, pagerKey)
    }
  }

  private[this] val stuckAgentMonitor = new Thread(abortingRunnable {
    try {
      while (!Thread.currentThread().isInterrupted) {
        val now = DateTime.now
        for ((agent, idx) <- agents.zipWithIndex) {
          val agentStart = agentStartTime(idx)
          // TODO: this should be configurable

          val incidentKey = "health/agent/stuck/%s".format(agent.label)
          if (agentStart + agent.period + agent.period + agent.period < now) {

            // Last start was three periods ago, probably not great.

            val description = "Stuck agent: %s" format agent.label
            val details = Map(
              "agent" -> agent.label,
              "last start" -> agentStart.toString(),
              "agent period" -> agent.period.toString()
            )
            raiseAlert(WARN, description, details)
            internalTriggerPager(incidentKey, description, details)
          } else {
            internalResolvePager(incidentKey)
          }
        }
        Thread.sleep(30000)
      }
    } catch {
      case e: InterruptedException =>
        // Suppress. We just want to print the log message below, and exit.
    }
    log.info("Interrupted. Bye!")
  }) withEffect {
    thread =>
      thread.setName("StuckAgentMonitor")
      thread.setDaemon(true)
  }

  def init() {
    stuckAgentMonitor.start()
    for ((agent, idx) <- agents.zipWithIndex) {
      def mkfn(thisTime: DateTime): Callable[_] = callable {

        val incidentKey = "health/agent/probe-failed/%s".format(agent.label)
        val again = try {
          agent synchronized {
            log.info("Probing: %s", agent.label)
            agentStartTime(idx) = DateTime.now
            agent.probe().withEffect { _ =>
              log.debug("Probe %s done in %s".format(agent.label, new Period(agentStartTime(idx), DateTime.now)))
              internalResolvePager(incidentKey)
            }
          }
        }
        catch {
          case e: Throwable =>
            val description = "Agent %s failed to make a probe" format agent.label
            val details = Map(
              "exceptionType" -> e.getClass.getName,
              "exceptionMessage" -> e.getMessage,
              "exceptionStackTrace" -> e.getStackTraceString
            )
            raiseAlert(e, WARN, description, details)
            internalTriggerPager(incidentKey, description, details)
            true
        }
        if (started && again) {
          // Schedule subsequent run
          val nextTime = thisTime + agent.period
          val pauseMillis = math.max(nextTime.getMillis - System.currentTimeMillis(), 0)
          exec.schedule(mkfn(nextTime), pauseMillis, TimeUnit.MILLISECONDS)
        } else {
          log.info("Removing agent: %s", agent.label)
        }
      }
      // Initialize agent.
      agent.init()
      // Schedule first run with some fuzziness.
      val now = new DateTime()
      val firstStart = new DateTime(now.getMillis + (((now + agent.period).getMillis - now.getMillis) * math.random).toLong)
      exec.schedule(mkfn(firstStart), firstStart.getMillis - now.getMillis, TimeUnit.MILLISECONDS)
    }
  }

  def close() {
    started = false
    stuckAgentMonitor.interrupt()
    agents foreach (_.close())
    exec.shutdown()
  }

  @LifecycleStart
  def start() = init()

  @LifecycleStop
  def stop() = close()
}
