package com.facetdata.health.statefulcheck

import com.facetdata.health.statefulcheck.StatefulCheckRunner.CheckResult
import com.google.common.base.Charsets
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RainerStatefulCheckTest extends FunSuite with MustMatchers
{
  test("Check function") {
    val checker = RainerStatefulCheck.deserialization.fromKeyAndBytes(
      "dummy",
      """
        |checks:
        |  dummyCheck:
        |    check: 'function() {
        |      var result = CheckResult.create("dummyCheck", true, {});
        |      var results = CheckResults.create([result]);
        |      return results;
        |    }'
        |    checkPeriod: PT10M
        |    failCount: 5
        |    resolveCount: 10
        |    pagerKey: somePagerKey
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val check = checker.checks("dummyCheck")
    check.name mustBe "dummyCheck"
    check.checkPeriod.get.getMinutes mustBe 10
    check.failCount mustBe 5
    check.resolveCount mustBe 10
    check.pagerKey.get mustBe "somePagerKey"
    check.checkFunction.apply().results.head mustBe CheckResult(name = "dummyCheck")
    check.definition mustBe Map(
      "check" ->
        """function() { var result = CheckResult.create("dummyCheck", true, {}); var results = CheckResults.create([result]); return results; }""",
      "checkPeriod" -> "PT10M",
      "failCount" -> 5,
      "resolveCount" -> 10,
      "pagerKey" -> "somePagerKey"
    )
  }
}