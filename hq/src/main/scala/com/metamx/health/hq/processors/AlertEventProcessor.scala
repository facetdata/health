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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.metamx.common.scala.Yaml
import com.metamx.common.scala.event._
import com.metamx.emitter.service.{ServiceEmitter, AlertEvent}
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.health.agent.events.HeartbeatEvent
import com.metamx.common.scala.Logging
import com.metamx.health.notify.MailBuffer
import com.metamx.health.notify.inventory.NotifyInventory
import java.util.Properties
import org.joda.time.DateTime
import org.slf4j.MDC
import org.yaml.snakeyaml.DumperOptions
import org.yaml.{snakeyaml => snake}
import scala.collection.JavaConverters._

@JsonIgnoreProperties(ignoreUnknown = true)
case class PreAlertEvent(
  timestamp:   String,
  service:     String,
  host:        String,
  severity:    String,
  description: String,
  data:        Map[String, AnyRef]
)
{
  def toAlertEvent: AlertEvent = {
    new AlertEvent(
      new DateTime(timestamp),
      service,
      host,
      Severity.valueOf(severity.toUpperCase.replace("-", "_")),
      description,
      data.asJava
    )
  }
}

object AlertEventProcessor extends Logging
{
  def logAlert(event: PreAlertEvent) {
    val priorMap = MDC.getCopyOfContextMap
    try {
      // we can convert the mapValues into json if there is a requirement. 
      val contextMap = priorMap.asScala ++
        event.data.mapValues("%s".format(_)) ++
        Map("timestamp" -> event.timestamp, "service" -> event.service, "host" -> event.host, "severity" -> event.severity)
      MDC.setContextMap(contextMap.asJava)
      log.debug(event.description)
    }
    finally {
      MDC.setContextMap(priorMap)
    }
  }
}

class AlertEventProcessor(notifyInventory: NotifyInventory, emitter: ServiceEmitter, topic: String, kafkaProps: Properties, notifySeverity: Severity)
  extends EventProcessor[PreAlertEvent](emitter, topic, kafkaProps)
{

  override def process(event: PreAlertEvent) {
    AlertEventProcessor.logAlert(event)
    val alertEvent = event.toAlertEvent
    if (alertEvent.getSeverity == notifySeverity)
    {
        notifyInventory withMailBuffer {
          mailBuffer => mailBuffer.add(MailBuffer.describe(alertEvent), mailBody(alertEvent))
        }
    }
  }

  override def metrics(event: PreAlertEvent): Option[Metric] = {
    val dimensions = Map(
      "service" -> Seq(event.service),
      "host" -> Seq(event.host),
      "severity" -> Seq(event.severity),
      "description" -> Seq(event.description)
    )
    Some(Metric("alert", 1, userDims = dimensions, created = new DateTime(event.timestamp)))
  }
  
  def mailBody(alertEvent: AlertEvent) = {
    // e.timestamp, e.host, then e.data sorted by key
    Seq(
      "timestamp" -> alertEvent.createdTime.toString(),
      "host"      -> alertEvent.host
    ) ++ alertEvent.dataMap.toSeq.sortBy(_._1) map { case (k,v) =>
      Yaml.dump(Map(k -> v), new snake.Yaml({
        val opts = new DumperOptions
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)         // Pretty print
        opts.setIndent(4)                                               // 4-space indent
        opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.LITERAL)   // better formatted line-breaks
        opts
      }))
    } mkString "\n"
  }
}
