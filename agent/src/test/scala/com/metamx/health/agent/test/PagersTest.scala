package com.metamx.health.agent.test

import com.fasterxml.jackson.databind.JsonMappingException
import com.metamx.common.scala.Jackson
import com.metamx.health.agent.events._
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, MustMatchers}

@RunWith(classOf[JUnitRunner])
class PagersTest extends FunSuite with MustMatchers
{
  test("PagerFromJsonError")
  {
    val json = """[]"""
    val e = intercept[JsonMappingException] {
      Jackson.parse[PagerEvent](json)
    }
    assert(e.isInstanceOf[JsonMappingException])
  }

  test("PagerTriggerFromJsonOkay")
  {
    val json =
      """
        |{
        | "timestamp": "2000",
        | "service": "lol",
        | "host": "localhost",
        | "page": true,
        | "data": {
        |   "type": "trigger",
        |   "incidentKey": "dude",
        |   "serviceKey": "yo",
        |   "description": "chill",
        |   "details": {
        |     "a": {"x": 1},
        |     "b": "yyy"
        |   }
        | }
        |}
      """.stripMargin
    val event = Jackson.parse[PagerEvent](json)
    event.getCreatedTime must be(new DateTime("2000"))
    event.service must be("lol")
    event.host must be("localhost")
    event.page must be(true)

    val data = PagerData.fromRaw(event.data)
    data must be(PagerTriggerData(
      incidentKey = "dude",
      serviceKey = "yo",
      description = "chill",
      details = Map("a" -> Map("x" -> 1), "b" -> "yyy")
    ))

    val json2 = Jackson.generate(event.toMap)
    val event2 = Jackson.parse[PagerEvent](json2)
    val data2 = PagerData.fromRaw(event2.data)
    event must be(event2)
    data must be(data2)
  }

  test("PagerResolveFromJsonOkay")
  {
    val json =
      """
        |{
        | "timestamp": "2000",
        | "service": "lol",
        | "host": "localhost",
        | "page": true,
        | "data": {
        |   "type": "resolve",
        |   "incidentKey": "dude",
        |   "serviceKey": "yo"
        | }
        |}
      """.stripMargin
    val event = Jackson.parse[PagerEvent](json)
    event.getCreatedTime must be(new DateTime("2000"))
    event.service must be("lol")
    event.host must be("localhost")
    event.page must be(true)

    val data = PagerData.fromRaw(event.data)
    data must be(PagerResolveData(
      incidentKey = "dude",
      serviceKey = "yo"
    ))

    val json2 = Jackson.generate(event.toMap)
    val event2 = Jackson.parse[PagerEvent](json2)
    val data2 = PagerData.fromRaw(event2.data)
    event must be(event2)
    data must be(data2)
  }
}
