package com.metamx.health.druid

import com.google.common.base.Charsets
import org.joda.time.Period
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, MustMatchers}


@RunWith(classOf[JUnitRunner])
class DruidTest extends FunSuite with MustMatchers
{
  test("Config deserialization") {
    val rainerDruid = RainerDruid.deserialization.fromKeyAndBytes(
      "foo",
      """
        |zkConnect: 'zk.example.com'
        |discoPath: '/prod/discovery'
        |broker: 'druid:broker'
        |coordinator: 'druid:coordinator'
        |alert:
        |  bar: PT1H
        |  baz:
        |    threshold: PT2H
        |  qux:
        |    threshold: PT3H
        |    pagerKey: hey
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    rainerDruid.zkConnect must be("zk.example.com")
    rainerDruid.discoPath must be("/prod/discovery")
    rainerDruid.broker must be("druid:broker")
    rainerDruid.coordinator must be("druid:coordinator")
    rainerDruid.thresholdAlert must be(
      Map(
        "bar" -> RainerDruidAlertPayload(new Period("PT1H"), None),
        "baz" -> RainerDruidAlertPayload(new Period("PT2H"), None),
        "qux" -> RainerDruidAlertPayload(new Period("PT3H"), Some("hey"))
      )
    )
  }
}
