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

package com.facetdata.health.statefulcheck

import com.facetdata.health.statefulcheck.StatefulCheckRunner.CheckResult
import com.facetdata.health.statefulcheck.StatefulCheckRunner.GroupAndCheckName
import com.facetdata.health.statefulcheck.StatefulCheckRunner.RunChecksResult
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.timekeeper.SystemTimekeeper
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.HeartbeatingAgent
import com.metamx.health.agent.PeriodicAgent
import com.metamx.health.core.config.ServiceConfig
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.rainer.Commit
import com.metamx.rainer.CommitKeeper
import com.twitter.util.Witness
import com.twitter.util.{Await => TwitterAwait}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.joda.time.Period
import org.scalatra.NotFound
import org.scalatra.Ok
import org.scalatra.ScalatraServlet
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class StatefulCheckAgent(
  val emitter: ServiceEmitter,
  val pagerInventory: Option[NotifyInventory],
  rainerKeeper: CommitKeeper[RainerStatefulCheck],
  config: HealthStatefulCheckConfig,
  serviceConfig: ServiceConfig
) extends PeriodicAgent with HeartbeatingAgent with StatefulCheckRunner
{
  private val allChecks = new AtomicReference[Map[Commit.Key, Commit[RainerStatefulCheck]]]()

  private lazy val closer = rainerKeeper.mirror().changes.register(Witness(allChecks))

  override def heartbeatPeriodMs: Long = config.heartbeatPeriod.toStandardDuration.getMillis

  override def period: Period = config.period

  override def label = "stateful checks"

  override protected val timeout: Duration = Duration(config.timeout.toStandardDuration.getMillis, TimeUnit.MILLISECONDS)

  override protected def timekeeper: Timekeeper = new SystemTimekeeper

  override def init() {
    // Eager initialization.
    closer
  }

  override def close() {
    TwitterAwait.result(closer.close())
  }

  override def probe(): Boolean = {
    dampedHeartbeat()
    val RunChecksResult(failed, resolved, _) = run()
    failed.foreach { case (baseCheck, checkResult) => fail(baseCheck, checkResult)}
    resolved.foreach { case (baseCheck, checkResult) => resolve(baseCheck, checkResult)}
    true
  }

  override protected def getChecksSnapshot: Map[GroupAndCheckName, StatefulCheck] = {
    for {
      (groupName, commit) <- allChecks.get()
      (_, check) <- commit.value match {
        case Some(Right(x)) => x.checks
        case None => Map.empty
        case Some(Left(e)) =>
          val msg = s"Bad config for stateful check group: $groupName"
          raiseAlert(e, Severity.SERVICE_FAILURE, msg, Map.empty[String, Any])
          Map.empty
      }
    } yield {
      (GroupAndCheckName(groupName, check.name), check)
    }
  }

  private def fail(
    baseCheck: StatefulCheck,
    checkResult: CheckResult
  ): Unit = {
    log.info(s"Check failed: ${baseCheck.group}/${checkResult.name} (data = ${checkResult.data})")
    val msg = s"Check failure: ${baseCheck.group}/${checkResult.name}"
    val details = Map[String, AnyRef](
      "check definition" -> baseCheck.definition,
      "result data" -> checkResult.data
    )
    raiseAlert(checkResult.name, serviceConfig.host, Severity.SERVICE_FAILURE, msg, details)
    baseCheck.pagerKey.foreach(triggerPager(checkResult.name, _, msg, details))
  }

  private def resolve(
    baseCheck: StatefulCheck,
    checkResult: CheckResult
  ) {
    log.info(s"Check recovered: ${baseCheck.group}/${checkResult.name} (data = ${checkResult.data})")
    baseCheck.pagerKey.foreach(resolvePager(checkResult.name, _))
  }

  val listServlet: ListServlet = new ListServlet

  class ListServlet extends ScalatraServlet with Logging
  {
    get("/:group/*") {
      checkStatuses.get(GroupAndCheckName(params("group"), multiParams("splat").mkString("/"))) match {
        case Some(checkStatus) =>
          val map = Map(
            "failed" -> checkStatus.failed,
            "lastRun" -> checkStatus.lastRun,
            "history" -> checkStatus.histories.asScala.map(_.ok)
          )
          contentType = "application/json"
          Ok(Jackson.generate(map))
        case None => NotFound("Not found!")
      }
    }
  }

}
