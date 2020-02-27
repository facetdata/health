package com.metamx.health.agent.test

import com.fasterxml.jackson.databind.JsonMappingException
import com.metamx.common.scala.Jackson
import com.metamx.health.agent.events.HeartbeatEvent
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, MustMatchers}

@RunWith(classOf[JUnitRunner])
class HeartbeatsTest extends FunSuite with MustMatchers
{
  test("HeartbeatFromJsonError")
  {
    val json = """[]"""
    val e = intercept[JsonMappingException] {
      Jackson.parse[HeartbeatEvent](json)
    }
    assert(e.isInstanceOf[JsonMappingException])
  }

  test("HeartbeatFromJsonOkay")
  {
    val json = """{"timestamp": "2000", "service": "lol", "host": "localhost", "agent": "007"}"""
    val event = Jackson.parse[HeartbeatEvent](json)
    event.getCreatedTime must be(new DateTime("2000"))
    event.service must be("lol")
    event.host must be("localhost")
    event.agent must be("007")

    val json2 = Jackson.generate(event.toMap)
    val event2 = Jackson.parse[HeartbeatEvent](json2)
    event must be(event2)
  }
}
