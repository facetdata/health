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

package com.metamx.health.notify

import com.metamx.common.scala.net.finagle.DefaultInetAddressResolver
import com.metamx.common.scala.net.uri._
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.{Jackson, Logging}
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.health.core.config.ServiceConfig
import com.metamx.health.util.AssertiveCloseable
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Method.Post
import com.twitter.finagle.http.Version.Http11
import com.twitter.finagle.http.{Http, Request, Response}
import com.twitter.finagle.{Group, Service}
import com.twitter.io.Buf.ByteArray
import com.twitter.util.{Await, Duration => TwitterDuration}
import java.net.URI
import com.github.nscala_time.time.Imports._

case class PagerDutyConfig(postUrl: URI, quiet: Boolean, mailOnly: Boolean)

class PagerDuty(mailBuffer: MailBuffer, config: PagerDutyConfig, serviceConfig: ServiceConfig)
  extends Logging with AssertiveCloseable
{
  private val client: Service[Request, Response] = ClientBuilder()
    .codec(Http())
    .hostConnectionLimit(1)
    .timeout(TwitterDuration.fromMilliseconds(20.seconds.standardDuration.millis))
    .daemon(true)
    .group(Group.fromVarAddr(DefaultInetAddressResolver.bind("%s:443" format config.postUrl.host)))
    .tls(config.postUrl.host)
    .build()

  if (config.quiet) {
    log.warn("%s: Starting in a quiet mode" format this)
  }
  else {
    log.info("%s: Starting" format this)
  }

  def trigger(incidentKey: String, serviceKey: String, description: String, details: Dict) {
    log.info("Trigger pager for incident[%s]".format(incidentKey))
    post(
      Map(
        "service_key" -> serviceKey,
        "incident_key" -> incidentKey,
        "event_type" -> "trigger",
        "description" -> description,
        "details" -> details
      )
    )
  }

  def resolve(incidentKey: String, serviceKey: String) {
    log.info("Resolve pager for incident[%s]".format(incidentKey))
    post(
      Map(
        "service_key" -> serviceKey,
        "incident_key" -> incidentKey,
        "event_type" -> "resolve"
      )
    )
  }

  protected override def assertiveClose() {
    log.info("%s: Closing" format this)
    Await.result(client.close())
    log.info("%s: Closed" format this)
  }

  private def post(data: Dict) {
    assertiveExecute {
      val json = Jackson.bytes(data)
      log.info("%s: %s" format(this, new String(json)))
      if (config.quiet) {
        log.warn("%s: Withholding pager (quiet = %s)".format(this, config.quiet))
      }
      else {
        if (config.mailOnly) {
          log.info("%s: Mailing pager (mailOnly = %s)".format(this, config.mailOnly))
          val subject = "PagerDuty event"
          val body = Jackson.pretty(data)
          mailBuffer.add(MailBuffer.subject(Severity.DEFAULT, serviceConfig.service, subject), body)
        } else {
          try {
            val request = Request(Http11, Post, config.postUrl.path)
            request.headerMap.set("Host", config.postUrl.authority)
            request.headerMap.set("Content-Type", "application/json")
            request.headerMap.set("Content-Length", json.length.toString)
            request.content = ByteArray.Shared(json)
            val response = Await.result(client(request))
            log.debug("%s: Got response: %s" format(this, response))
            if (response.statusCode / 100 != 2) {
              throw new IllegalStateException(
                "Non-2xx status code: %s %s" format
                  (response.statusCode, response.status.reason)
              )
            }
          }
          catch {
            // (Non-2xx response codes throw an error)
            case e: Exception =>
              val subject = "Failed to post PagerDuty event"
              val body = "%s\n\n%s\n%s" format(new String(json), e, e.getStackTraceString)

              log.error("%s: %s: %s" format(this, subject, body))
              mailBuffer.add(MailBuffer.subject(Severity.SERVICE_FAILURE, serviceConfig.service, subject), body)
          }
        }
      }
    }
  }

  override def toString = "PagerDuty"
}
