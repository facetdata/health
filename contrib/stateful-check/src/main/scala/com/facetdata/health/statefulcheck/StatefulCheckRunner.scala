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
import com.facetdata.health.statefulcheck.StatefulCheckRunner.CheckResults
import com.facetdata.health.statefulcheck.StatefulCheckRunner.CheckStatus
import com.facetdata.health.statefulcheck.StatefulCheckRunner.GroupAndCheckName
import com.facetdata.health.statefulcheck.StatefulCheckRunner.RunChecksResult
import com.github.nscala_time.time.Imports.DateTime
import com.metamx.common.scala.Logging
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped.Dict
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success

trait StatefulCheckRunner extends Logging
{
  protected val timeout: Duration

  protected def timekeeper: Timekeeper

  protected def getChecksSnapshot: Map[GroupAndCheckName, StatefulCheck]

  protected val checkStatuses: concurrent.Map[GroupAndCheckName, CheckStatus] =
    new ConcurrentHashMap[GroupAndCheckName, CheckStatus]().asScala

  def run(): RunChecksResult = {
    val allChecksSnapshot: Map[GroupAndCheckName, StatefulCheck] = getChecksSnapshot
    // Run all checks.
    val checkResultsFutures = runChecks(allChecksSnapshot)
    val checkResults = Await.result(Future.sequence(checkResultsFutures), timeout)
    // When results come back, update histories
    processCheckResults(allChecksSnapshot, checkResults)
  }

  private def runChecks(allChecksSnapshot: Map[GroupAndCheckName, StatefulCheck]) = {
    val start = timekeeper.now
    for (
      (groupAndCheck, statefulCheck) <- allChecksSnapshot
      if statefulCheck.enabled &&
        ((statefulCheck.checkPeriod, checkStatuses.get(groupAndCheck).flatMap(_.lastRun)) match {
          case (Some(checkPeriod), Some(lastRun)) => lastRun.withPeriodAdded(checkPeriod, 1).isBefore(start)
          case _ => true
        })
    ) yield {
      Future(statefulCheck.checkFunction.apply())
        .map(Success(_))
        .recover { case t => Failure(t) }
        .map {
          case Success(checkResults) =>
            (
              groupAndCheck,
              checkResults.copy(results = CheckResult(name = statefulCheck.name) +: checkResults.results),
              timekeeper.now.getMillis - start.getMillis
            )
          case Failure(e) =>
            (
              groupAndCheck,
              CheckResults(
                Array(CheckResult(name = statefulCheck.name, ok = false, data = Map("error" -> e.getMessage)))
              ),
              timekeeper.now.getMillis - start.getMillis
            )
        }
    }
  }

  private def processCheckResults(
    allChecksSnapshot: Map[GroupAndCheckName, StatefulCheck],
    results: Iterable[(GroupAndCheckName, CheckResults, Long)]
  ) = {
    val failedChecks, resolvedChecks, otherChecks = ListBuffer.empty[(StatefulCheck, CheckResult)]
    for ((baseGroupAndCheck, checksResult, latency) <- results) yield {
      val baseCheck = allChecksSnapshot(baseGroupAndCheck)
      for (checkResult <- checksResult.results) yield {
        val groupAndCheck = baseGroupAndCheck.copy(checkName = checkResult.name)
        val checkStatus = checkStatuses.getOrElseUpdate(groupAndCheck, new CheckStatus(checkResult.name))
        val histories = checkStatus.histories
        checkStatus.lastRun = Some(timekeeper.now)
        histories.add(checkResult)
        while (histories.size > math.max(baseCheck.failCount, baseCheck.resolveCount)) {
          histories.poll()
        }

        val historyAsScala = histories.asScala
        log.info(
          "get %s/%s: %s (%,dms) (histories = %s)",
          groupAndCheck.group,
          groupAndCheck.checkName,
          if (checkResult.ok) "ok" else "NOT ok",
          latency,
          historyAsScala.map(_.ok).mkString(", ")
        )

        val isFailed =
          histories.size >= baseCheck.failCount &&
            historyAsScala.takeRight(baseCheck.failCount).forall(!_.ok)

        val isResolved =
          checkStatus.failed &&
            histories.size >= baseCheck.resolveCount &&
            historyAsScala.takeRight(baseCheck.resolveCount).forall(_.ok)

        if (isFailed) {
          failedChecks += ((baseCheck, checkResult))
          checkStatus.failed = true
        } else if (isResolved) {
          resolvedChecks += ((baseCheck, checkResult))
          checkStatus.failed = false
        } else {
          otherChecks += ((baseCheck, checkResult))
        }
      }
    }
    RunChecksResult(failed = failedChecks.toList, resolved = resolvedChecks.toList, others = otherChecks.toList)
  }
}

object StatefulCheckRunner {
  case class CheckResult(
    name: String,
    ok: Boolean = true,
    data: Dict = Map.empty
  )
  case class CheckResults(
    results: Array[CheckResult]
  )
  case class GroupAndCheckName(group: String, checkName: String)
  class CheckStatus(
    val name: String,
    val histories: ConcurrentLinkedQueue[CheckResult] = new ConcurrentLinkedQueue(),
    @volatile var lastRun: Option[DateTime] = None,
    @volatile var failed: Boolean = false
  )
  case class RunChecksResult(
    failed: List[(StatefulCheck, CheckResult)],
    resolved: List[(StatefulCheck, CheckResult)],
    others: List[(StatefulCheck, CheckResult)]
  )
}
