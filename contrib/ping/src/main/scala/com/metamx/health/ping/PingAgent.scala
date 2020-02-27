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

package com.metamx.health.ping

import com.github.nscala_time.time.Imports._
import com.google.common.base.Charsets
import com.google.common.base.Throwables
import com.metamx.common.IAE
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.event._
import com.metamx.common.scala.net.curator._
import com.metamx.common.scala.net.finagle.DefaultInetAddressResolver
import com.metamx.common.scala.net.finagle.DiscoResolver
import com.metamx.common.scala.net.uri._
import com.metamx.common.scala.option._
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.untyped.str
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.HeartbeatingAgent
import com.metamx.health.agent.PeriodicAgent
import com.metamx.health.core.config.ServiceConfig
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.rainer.Commit
import com.metamx.rainer.CommitKeeper
import com.twitter.finagle.Addr
import com.twitter.finagle.GlobalRequestTimeoutException
import com.twitter.finagle.Group
import com.twitter.finagle.NoBrokersAvailableException
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Method.Get
import com.twitter.finagle.http.Method.Post
import com.twitter.finagle.http.Version.Http11
import com.twitter.finagle.http._
import com.twitter.io.Buf.ByteArray
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Var
import com.twitter.util.Witness
import com.twitter.util.{Duration => TwitterDuration}
import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import org.apache.commons.codec.binary.Base64
import org.scalatra.NotFound
import org.scalatra.Ok
import org.scalatra.ScalatraServlet
import scala.collection.JavaConverters._

class PingAgent(
  val emitter: ServiceEmitter,
  val pagerInventory: Option[NotifyInventory],
  discotheque: Discotheque,
  rainerKeeper: CommitKeeper[RainerPing],
  config: HealthPingConfig,
  serviceConfig: ServiceConfig
) extends PeriodicAgent with HeartbeatingAgent
{

  implicit def jodaDurationToTwitterDuration(duration: Duration): TwitterDuration = {
    TwitterDuration.fromMilliseconds(duration.getMillis)
  }

  case class GroupAndService(group: String, service: String)

  class ClientAndHistory(
    val uri: URI,
    val client: Service[Request, Response],
    val histories: ConcurrentLinkedQueue[Either[Throwable, PingResponse]],
    @volatile var lastRun: Option[DateTime],
    @volatile var failed: Boolean
  )

  private val allServices       = new AtomicReference[Map[Commit.Key, Commit[RainerPing]]]()

  private val timeout: Duration = 10.seconds
  private val clients           = new ConcurrentHashMap[GroupAndService, ClientAndHistory]().asScala

  private lazy val closer = rainerKeeper.mirror().changes.register(Witness(allServices))

  override def heartbeatPeriodMs = config.heartbeatPeriod.toStandardDuration.getMillis

  override def period = config.period

  override def label = "http pings"

  override def init() {
    // Force initialization.
    closer
  }

  override def probe() = {
    dampedHeartbeat()

    val allServicesSnapshot: Map[GroupAndService, PingableService] = {
      for {
        (groupName, commit) <- allServices.get()
        (_, service) <- commit.value match {
          case Some(Right(x)) => x.services
          case None => Map.empty
          case Some(Left(e)) =>
            val msg = "Bad config for ping group: %s" format groupName
            raiseAlert(e, Severity.SERVICE_FAILURE, msg, Map.empty[String, Any])
            Map.empty
        }
      } yield {
        (GroupAndService(groupName, service.name), service)
      }
    }

    // Adjust clients and histories.
    for ((oldKey, oldClient) <- clients) {
      if (!allServicesSnapshot.contains(oldKey) || allServicesSnapshot(oldKey).uri != oldClient.uri) {
        log.info("No longer monitoring service: %s/%s (%s).", oldKey.group, oldKey.service, oldClient.uri)
        clients(oldKey).client.close() // Close asynchronously, ignore return value
        clients.remove(oldKey)
      }
    }

    for (newKey <- allServicesSnapshot.keySet -- clients.keySet) {
      val service = allServicesSnapshot(newKey)
      val uri = service.uri

      log.info("Starting monitor for service: %s/%s (%s).", newKey.group, newKey.service, uri)

      clients(newKey) = new ClientAndHistory(
        uri,
        makeClient(uri, service),
        new ConcurrentLinkedQueue[Either[Throwable, PingResponse]],
        None,
        false
      )
    }

    // Ping all services.
    val start = System.currentTimeMillis()
    val futures: Iterable[Future[(GroupAndService, (Either[Throwable, PingResponse], Long))]] = {
      for ((groupAndService, service) <- allServicesSnapshot
           if ((service.pingPeriod, clients(groupAndService).lastRun) match {
             case (Some(pingPeriod), Some(lastRun)) => lastRun.withPeriodAdded(pingPeriod, 1).isBefore(start)
             case _ => true
           })) yield {
        val client = clients(groupAndService).client
        val method = if (service.postData.isDefined) {
          Post
        } else {
          Get
        }
        val path = service.uri.getRawPath +
                   Option(service.uri.getRawQuery).map("?" + _).getOrElse("") +
                   Option(service.uri.getRawFragment).map("#" + _).getOrElse("")

        val request = Request(Http11, method, path) withEffect {
          req =>
            req.headerMap.set("Host", Option(service.uri.host).getOrElse("0.0.0.0"))
            req.headerMap.set("Accept", "*/*")
            Option(service.uri.userInfo) ifDefined {
              authorization =>
                req.headerMap.set(
                  "Authorization",
                  "Basic %s" format Base64.encodeBase64String(authorization.getBytes(Charsets.UTF_8))
                )
            }
            service.postData ifDefined {
              postData =>
                val bytes = postData.apply().getBytes(Charsets.UTF_8)
                req.headerMap.set("Content-Type", "application/json")
                req.headerMap.set("Content-Length", bytes.size.toString)
                req.content = ByteArray.Shared(bytes)
            }
        }
        client(request).flatMap { response =>
          // its mainly for temporary redirects 307, we can have multiple cases here like 302 etc also.
          if (response.status != Status.TemporaryRedirect) {
            Future.value(response)
          } else {
            var uri = new URI(response.location.get)
            // response.location returns without any scheme for marathon
            if (uri.getScheme == null) {
              uri = uri.withScheme(service.uri.getScheme)
            }
            val newClient = makeClient(uri, service)
            newClient(request)
          }
        } transform {
          case Return(response) =>
            val pingResponse = PingResponse(response.statusCode, response.contentString)
            Future((groupAndService, (Right(pingResponse), System.currentTimeMillis() - start)))

          case Throw(e) =>
            Future((groupAndService, (Left(e): Either[Throwable, PingResponse], System.currentTimeMillis() - start)))
        }
      }
    }

    // When results come back, send metrics, update histories, and possibly send alerts.
    val results = Await.result(Future.collect(futures.toList))
    for ((groupAndService, (response, latency)) <- results) {
      val pingableService = allServicesSnapshot(groupAndService)
      val clientAndHistory = clients(groupAndService)
      val histories = clientAndHistory.histories
      val statusCode = response.fold(_ => "failed-fetch", _.statusCode.toString)
      val pingResult = applyServicePredicate(pingableService, response)
      clientAndHistory.lastRun = Some(DateTime.now)

      // Update histories.
      histories.add(response)
      while (histories.size > math.max(pingableService.failCount, pingableService.resolveCount)) {
        histories.poll()
      }

      val historyAsScala = histories.asScala
      // Send metrics.
      log.info(
        "get %s/%s (%s): %s (%,dms) (%s; histories = %s)",
        groupAndService.group,
        groupAndService.service,
        pingableService.uri,
        statusCode,
        latency,
        if (pingResult.ok) "ok" else "NOT ok",
        historyAsScala.map(statusCodeString).mkString(", ")
      )
      val metricBase = Metric(
        userDims = Map(
          "pingServiceName" -> Seq(pingableService.name),
          "pingStatusCode" -> Seq(statusCode),
          "pingGroup" -> Seq(groupAndService.group),
          "pingResult" -> Seq(pingResult.ok.toString)
        )
      )
      Seq(
        Metric("get", -1),
        Metric("latency", latency)
      ) map (metricBase + _) foreach reportMetrics

      // Possibly send alerts.
      val isFailure =
        histories.size >= pingableService.failCount &&
        historyAsScala.takeRight(pingableService.failCount).forall(!applyServicePredicate(pingableService, _).ok)

      val isResolved =
        clientAndHistory.failed &&
          histories.size >= pingableService.resolveCount &&
          historyAsScala.takeRight(pingableService.resolveCount).forall(applyServicePredicate(pingableService, _).ok)

      if (isFailure) {
        fail(groupAndService, pingableService, historyAsScala.toList, pingResult.data)
        clientAndHistory.failed = true
      } else if (isResolved) {
        resolve(groupAndService, pingableService, historyAsScala.toList, pingResult.data)
        clientAndHistory.failed = false
      }
    }
    true
  }

  override def close() {
    Await.result(closer.close())
  }

  private def fail(
    groupAndService: GroupAndService,
    pingableService: PingableService,
    histories: Seq[Either[Throwable, PingResponse]],
    data: Dict
  )
  {
    log.info("Service failed: %s/%s (data = %s)", groupAndService.group, groupAndService.service, data)

    // Send an alert, and maybe a page.
    val severity = Severity.SERVICE_FAILURE
    val msg = "Service failure: %s/%s" format (groupAndService.group, groupAndService.service)
    val mostRecentBody: Option[String] = histories.reverse.find(_.isRight).map(_.right.get.body)
    val mostRecentException: Option[Throwable] = histories.reverse.find(_.isLeft).map(_.left.get)
    val details = Map[String, AnyRef](
      "uri" -> sanitize(pingableService.uri),
      "check definition" -> (pingableService.definition map {
        // Weak attempt to sanitize the definitions.
        case (k, v) if k == "uri" =>
          try {
            (k, sanitize(new URI(str(v))))
          } catch {
            case e: Exception =>
              (k, v)
          }
        case (k, v) =>
          (k, v)
      }),
      "status history" -> histories.map(statusCodeString).mkString(", ")
    ) ++ (mostRecentException map {
      e =>
        "exception" -> Throwables.getStackTraceAsString(e)
    }) ++ (mostRecentBody map {
      b =>
        "body" -> trimBody(b)
    }) ++ (if (data.nonEmpty) {
      Map("check data" -> data)
    } else {
      Map.empty
    })

    // Don't raise alerts if failed-fetch due to timeout
    (mostRecentException, pingableService.ignoreFailedFetch) match {
      case (Some(_: GlobalRequestTimeoutException), true) | (Some(_: NoBrokersAvailableException), true)=>
        log.warn("Ignoring failed-fetch for %s [%s] %s (%s)", groupAndService.service, serviceConfig.host, msg, details)

      case _ =>
        raiseAlert(groupAndService.service, serviceConfig.host, severity, msg, details)
        pingableService.pagerKey ifDefined { pagerKey => triggerPager(groupAndService.service, pagerKey, msg, details) }
    }
  }

  private def resolve(
    groupAndService: GroupAndService,
    pingableService: PingableService,
    histories: Seq[Either[Throwable, PingResponse]],
    data: Dict
  )
  {
    log.info("Service recovered: %s/%s (data = %s)", groupAndService.group, groupAndService.service, data)

    // Resolve is only useful for PagerDuty.
    pingableService.pagerKey ifDefined {
      pagerKey =>
        resolvePager(groupAndService.service, pagerKey)
    }
  }

  private def statusCodeString(either: Either[Throwable, PingResponse]) = {
    either.fold(_ => "failed-fetch", _.statusCode.toString)
  }

  private def applyServicePredicate(
    service: PingableService,
    either: Either[Throwable, PingResponse]
  ): PingResult =
  {
    either match {
      case Left(e) => PingResult(false)
      case Right(r) =>
        try service.predicate(r)
        catch {
          case e: Exception =>
            raiseAlert(
              e, Severity.SERVICE_FAILURE, "Failed to run service predicate: %s" format service.name, Map(
                "serviceName" -> service.name,
                "responseBody" -> trimBody(r.body),
                "responseCode" -> r.statusCode
              )
            )
            PingResult(false)
        }
    }
  }

  private def trimBody(body: String) = {
    body.take(1024) + (if (body.size > 1024) " ..." else "")
  }

  private def sanitize(uri: URI): URI = {
    if (Option(uri.userInfo).isDefined) {
      val sanitizedUserInfo = if (uri.userInfo contains ":") {
        uri.userInfo.replaceAll("""\:.*""", ":*")
      } else {
        "*"
      }
      uri.withUserInfo(sanitizedUserInfo)
    } else {
      uri
    }
  }

  private def makeClient(uri: URI, service: PingableService): Service[Request, Response] = {
    val target: Var[Addr] = uri.scheme match {
      case "http" =>
        val port = if (uri.port > 0) uri.port else 80
        DefaultInetAddressResolver.bind("%s:%s" format(uri.host, port))

      case "https" =>
        val port = if (uri.port > 0) uri.port else 443
        DefaultInetAddressResolver.bind("%s:%s" format(uri.host, port))

      case "disco" =>
        val discoConfig = service.discoConfig.getOrElse(throw new IAE("discoConfig should be defined for disco URI"))
        new DiscoResolver(discotheque.disco(discoConfig)).bind(uri.authority)
    }
    val preTlsBuilder = ClientBuilder()
      .name("%s:%s" format(uri.scheme, uri.authority))
      .codec(Http())
      .group(Group.fromVarAddr(target))
      .hostConnectionLimit(1)
      .tcpConnectTimeout(timeout)
      .timeout(timeout)
      .daemon(true)
    if (uri.scheme == "https") {
      // TODO: Support https for disco URLs, somehow.
      preTlsBuilder.tls(uri.host).build()
    } else {
      preTlsBuilder.build()
    }
  }

  // TODO: move this logic to status checks
  val listServlet = new ListServlet {}

  private def responseMap(either: Either[Throwable, PingResponse]) =
  {
    either.fold(
      _ => Map("success" -> false, "reason" -> "failed-fetch"),
      x => Map("success" -> true, "statusCode" -> x.statusCode, "body" -> x.body)
    )
  }

  class ListServlet extends ScalatraServlet with Logging
  {
    get("/:group/*")
    {
      clients.get(GroupAndService(params("group"), multiParams("splat").mkString("/"))) match {
        case Some(client) => {
          val map = Map(
            "failed" -> client.failed,
            "uri" -> client.uri,
            "lastRun" -> client.lastRun,
            "history" -> client.histories.asScala.map(responseMap)
          )
          contentType = "application/json"
          Ok(Jackson.generate(map))
        }
        case None => NotFound("Not found!")
      }
    }
  }
}
