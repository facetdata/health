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

import com.metamx.common.scala.event.Metric
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.events._
import com.metamx.health.notify.inventory.NotifyInventory
import java.util.Properties

class PagerEventProcessor(notifyInventory: NotifyInventory, emitter: ServiceEmitter, topic: String, kafkaProps: Properties)
  extends EventProcessor[PagerEvent](emitter, topic, kafkaProps)
{
  override def process(event: PagerEvent) {
    val action = if (event.page) {
      "pagerduty"
    } else {
      "log only"
    }

    val data = PagerData.fromRaw(event.data)
    data match {
      case x: PagerTriggerData =>
        log.info("Received pager trigger: [%s] %s %s | action=%s | incident=%s service=%s >> %s".format(
          event.timestamp, event.service, event.host, action, x.incidentKey, x.serviceKey, x.description
        ))
        if (event.page) {
          notifyInventory withPagerDuty {
            pd => pd.trigger(x.incidentKey, x.serviceKey, x.description, x.details)
          }
        }

      case x: PagerResolveData =>
        log.info("Received pager resolve: [%s] %s %s | %s | incident=%s service=%s".format(
          event.timestamp, event.service, event.host, action, x.incidentKey, x.serviceKey
        ))
        if (event.page) {
          notifyInventory withPagerDuty {
            od => od.resolve(x.incidentKey, x.serviceKey)
          }
        }
    }
  }

  override def metrics(event: PagerEvent): Option[Metric] = {
    val data = PagerData.fromRaw(event.data)
    val (metric, dimensions) = data match {
      case x: PagerTriggerData =>
        ("pager/trigger", Map(
          "service" -> Seq(event.service),
          "host" -> Seq(event.host),
          "page" -> Seq(event.page.toString),
          "serviceKey" -> Seq(x.serviceKey),
          "description" -> Seq(x.description)
        ))

      case x: PagerResolveData =>
        ("pager/resolve", Map(
          "service" -> Seq(event.service),
          "host" -> Seq(event.host),
          "page" -> Seq(event.page.toString),
          "serviceKey" -> Seq(x.serviceKey)
        ))
    }
    Some(Metric(metric, 1, userDims = dimensions, created = event.timestampTT))
  }
}
