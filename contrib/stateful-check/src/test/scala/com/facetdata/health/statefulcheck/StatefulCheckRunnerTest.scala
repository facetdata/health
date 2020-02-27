package com.facetdata.health.statefulcheck

import com.facetdata.health.statefulcheck.StatefulCheckRunner.CheckResult
import com.facetdata.health.statefulcheck.StatefulCheckRunner.CheckResults
import com.facetdata.health.statefulcheck.StatefulCheckRunner.GroupAndCheckName
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.common.scala.timekeeper.Timekeeper
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class StatefulCheckRunnerTest extends FunSuite with MustMatchers
{

  test("base & sub checks: success") {
    val testingTimekeeper = new TestingTimekeeper
    testingTimekeeper.now = new DateTime(0)
    val check = StatefulCheck(
      group = "1",
      name = "check-1",
      pagerKey = None,
      checkFunction = () => CheckResults(Array(CheckResult("check-1-1"))),
      checkPeriod = None,
      failCount = 1,
      resolveCount = 1,
      definition = Map.empty
    )
    val runner = new StatefulCheckRunner
    {
      override protected val timeout: Duration = Duration.Inf

      override protected def timekeeper: Timekeeper = testingTimekeeper

      override protected def getChecksSnapshot: Map[GroupAndCheckName, StatefulCheck] = {
        Map(GroupAndCheckName("1", check.name) -> check)
      }
    }
    val result = runner.run()
    result.failed mustBe empty
    result.resolved mustBe empty
    result.others.size mustBe 2
    result.others.exists(_._2.name == "check-1") mustBe true
    result.others.exists(_._2.name == "check-1-1") mustBe true
  }

  test("base check: fail") {
    val testingTimekeeper = new TestingTimekeeper
    testingTimekeeper.now = new DateTime(0)
    val check = StatefulCheck(
      group = "1",
      name = "check-1",
      pagerKey = None,
      checkFunction = () => throw sys.error("dummy"),
      checkPeriod = None,
      failCount = 1,
      resolveCount = 1,
      definition = Map.empty
    )
    val runner = new StatefulCheckRunner
    {
      override protected val timeout: Duration = Duration.Inf

      override protected def timekeeper: Timekeeper = testingTimekeeper

      override protected def getChecksSnapshot: Map[GroupAndCheckName, StatefulCheck] = {
        Map(GroupAndCheckName("1", check.name) -> check)
      }
    }
    val result = runner.run()
    result.failed.size mustBe 1
    result.failed.exists(_._2.name == "check-1") mustBe true
    result.resolved mustBe empty
    result.others mustBe empty
  }

  test("sub check: fail") {
    val testingTimekeeper = new TestingTimekeeper
    testingTimekeeper.now = new DateTime(0)
    val check = StatefulCheck(
      group = "1",
      name = "check-1",
      pagerKey = None,
      checkFunction = () => CheckResults(Array(CheckResult("check-1-1", ok = false))),
      checkPeriod = None,
      failCount = 1,
      resolveCount = 1,
      definition = Map.empty
    )
    val runner = new StatefulCheckRunner
    {
      override protected val timeout: Duration = Duration.Inf

      override protected def timekeeper: Timekeeper = testingTimekeeper

      override protected def getChecksSnapshot: Map[GroupAndCheckName, StatefulCheck] = {
        Map(GroupAndCheckName("1", check.name) -> check)
      }
    }
    val result = runner.run()
    result.failed.size mustBe 1
    result.failed.exists(_._2.name == "check-1-1") mustBe true
    result.resolved mustBe empty
    result.others.size mustBe 1
    result.others.exists(_._2.name == "check-1") mustBe true
  }

  test("base check failed & resolved") {
    val testingTimekeeper = new TestingTimekeeper
    testingTimekeeper.now = new DateTime(0)
    var ok = false
    val check = StatefulCheck(
      group = "1",
      name = "check-1",
      pagerKey = None,
      checkFunction = () => if (!ok) throw sys.error("dummy") else CheckResults(Array()),
      checkPeriod = None,
      failCount = 1,
      resolveCount = 1,
      definition = Map.empty
    )
    val runner = new StatefulCheckRunner
    {
      override protected val timeout: Duration = Duration.Inf

      override protected def timekeeper: Timekeeper = testingTimekeeper

      override protected def getChecksSnapshot: Map[GroupAndCheckName, StatefulCheck] = {
        Map(GroupAndCheckName("1", check.name) -> check)
      }
    }
    runner.run()
    ok = true
    val result = runner.run()
    result.failed mustBe empty
    result.resolved.size mustBe 1
    result.resolved.exists(_._2.name == "check-1") mustBe true
    result.others mustBe empty
  }

  test("sub check failed & resolved") {
    val testingTimekeeper = new TestingTimekeeper
    testingTimekeeper.now = new DateTime(0)
    var ok = false
    val check = StatefulCheck(
      group = "1",
      name = "check-1",
      pagerKey = None,
      checkFunction = () => CheckResults(Array(CheckResult("check-1-1", ok))),
      checkPeriod = None,
      failCount = 1,
      resolveCount = 1,
      definition = Map.empty
    )
    val runner = new StatefulCheckRunner
    {
      override protected val timeout: Duration = Duration.Inf

      override protected def timekeeper: Timekeeper = testingTimekeeper

      override protected def getChecksSnapshot: Map[GroupAndCheckName, StatefulCheck] = {
        Map(GroupAndCheckName("1", check.name) -> check)
      }
    }
    runner.run()
    ok = true
    val result = runner.run()
    result.failed mustBe empty
    result.resolved.size mustBe 1
    result.resolved.exists(_._2.name == "check-1-1") mustBe true
    result.others.size mustBe 1
    result.others.exists(_._2.name == "check-1") mustBe true
  }

}
