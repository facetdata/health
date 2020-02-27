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

import com.metamx.common.scala.Logging
import com.metamx.common.scala.event.Metric
import com.metamx.common.scala.event.emit
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.AlertEvent
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.events._
import com.metamx.health.notify.inventory.NotifyInventory
import java.io.Closeable

trait Agent extends Logging with Closeable
{
  def label: String

  def emitter: ServiceEmitter

  // If this has value, pager alerts will be sent by the agent itself and not the health headquarter.
  // In this case, pager events will still be emitted to headquarter but used only for bookkeeping purposes.
  def pagerInventory: Option[NotifyInventory]

  def init()

  def close()

  def reportMetrics(metrics: Seq[Metric]) {
    for (metric <- metrics) {
      reportMetrics(metric)
    }
  }

  def reportMetrics(metric: Metric) {
    emitter.emit(metric)
  }

  def updateStatus(track: IndexedSeq[String], tags: Map[String, String], value: String, message: String) {
    emitter.emit(new StatusEventBuilder(label, track, tags, value, message))
  }

  def raiseAlert(severity: Severity, description: String, data: Dict) {
    raiseAlert(None, severity, description, data)
  }

  def raiseAlert(e: Throwable, severity: Severity, description: String, data: Dict) {
    raiseAlert(Option(e), severity, description, data)
  }

  def raiseAlert(service: String, host: String, severity: Severity, description: String, data: Dict) {
    emitter.emit(
      new AlertEvent(service, host, severity, description, normalizeJava(data).asInstanceOf[java.util.Map[String, AnyRef]])
    )
  }

  def raiseAlert(e: Option[Throwable], severity: Severity, description: String, data: Dict) {
    emit.emitAlert(e.orNull, log, emitter, severity, description, data)
  }

  def triggerPager(incidentKey: String, serviceKey: String, description: String, details: Dict) {
    val page = pagerInventory match {
      case Some(x) =>
        try {
          x withPagerDuty { pd => pd.trigger(incidentKey, serviceKey, description, details) }
          false
        } catch {
          case e: Throwable =>
            log.error("Unable to directly call pagerduty, falling back to pager events")
            true
        }
        
      case None =>
        true
    }
    val data = PagerTriggerData(incidentKey, serviceKey, description, details)
    val raw = PagerData.toRaw(data)
    emitter.emit(new PagerEventBuilder(page, raw))
  }

  def resolvePager(incidentKey: String, serviceKey: String) {
    val page = pagerInventory match {
      case Some(x) =>
        try {
          x withPagerDuty { pd => pd.resolve(incidentKey, serviceKey) }
          false
        } catch {
          case e: Throwable =>
            log.error("Unable to directly call pagerduty, falling back to pager events")
            true
        }

      case None =>
        true
    }
    val data = PagerResolveData(incidentKey, serviceKey)
    val raw = PagerData.toRaw(data)
    emitter.emit(new PagerEventBuilder(page, raw))
  }

  def heartbeat() {
    emitter.emit(new HeartbeatEventBuilder(label))
  }
}
