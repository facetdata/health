package com.metamx.health.agent.test

import com.fasterxml.jackson.databind.JsonMappingException
import com.metamx.common.scala.Jackson
import com.metamx.health.agent.events.StatusEvent
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, MustMatchers}

@RunWith(classOf[JUnitRunner])
class StatusesTest extends FunSuite with MustMatchers
{
  test("StatusFromJsonError")
  {
    val json = """[]"""
    val e = intercept[JsonMappingException] {
      Jackson.parse[StatusEvent](json)
    }
    assert(e.isInstanceOf[JsonMappingException])
  }

  test("StatusFromJsonOkay")
  {
    val json =
      """
        |{
        | "timestamp": "2000",
        | "service": "lol",
        | "host": "localhost",
        | "agent": "007",
        | "track": ["a", "b", "c"],
        | "tags": {"x": 1, "y": 2},
        | "value": "31337",
        | "message": "rofl"
        |}
      """.stripMargin
    val event = Jackson.parse[StatusEvent](json)
    event.getCreatedTime must be(new DateTime("2000"))
    event.service must be("lol")
    event.host must be("localhost")
    event.agent must be("007")
    event.track must be(Seq("a", "b", "c"))
    event.tags must be(Map("x" -> "1", "y" -> "2"))
    event.value must be("31337")
    event.message must be("rofl")


    val json2 = Jackson.generate(event.toMap)
    val event2 = Jackson.parse[StatusEvent](json2)
    event must be(event2)
  }
}
