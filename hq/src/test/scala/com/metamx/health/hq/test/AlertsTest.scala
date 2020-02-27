package com.metamx.health.hq.test

import java.io.StringWriter
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.untyped.Dict
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.health.hq.processors.{AlertEventProcessor, PreAlertEvent}
import org.apache.logging.log4j.core.config.DefaultConfiguration
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.WriterAppender
import org.apache.logging.log4j.core.layout.JsonLayout
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, MustMatchers}

@RunWith(classOf[JUnitRunner])
class AlertsTest extends FunSuite with MustMatchers
{
  test("AlertFromJsonError")
  {
    val json = """{"timestamp": "hey", "data": {"hey": "what"}}"""
    val e = intercept[IllegalArgumentException] {
      Jackson.parse[PreAlertEvent](json).toAlertEvent
    }
    assert(e.isInstanceOf[IllegalArgumentException])
  }

  test("AlertFromJsonOkay")
  {
    val json = """{"timestamp": "2000", "service": "test", "host": "localhost", "junk": "no", "severity": "anomaly", "data": {"hey": "what"}}"""
    val event = Jackson.parse[PreAlertEvent](json).toAlertEvent
    event.getCreatedTime must be(new DateTime("2000"))
    event.getDataMap.get("hey") must be("what")
    event.getSeverity must be(Severity.ANOMALY)
    event.getHost must be("localhost")
    event.getService must be("test")
  }

  test("LogPreAlert")
  {
    val context = LoggerContext.getContext(false)
    val config = context.getConfiguration
    val layout = JsonLayout.createLayout(new DefaultConfiguration(), false, true, false, false, true, true, null, null, null)
    val writer = new StringWriter
    val appender = WriterAppender.createAppender(layout, null, writer, "writer", false, true)
    appender.start()
    config.getLoggerConfig("com.metamx.health.hq.processors.AlertEventProcessor$").addAppender(appender, Level.DEBUG, null)
    context.updateLoggers()

    val event = PreAlertEvent(
      timestamp = "2016-09-23T00:00:00Z",
      service = "health-test",
      host = "127.0.0.1",
      severity = "anomaly",
      description = "describe me",
      data = Map(
        "hey" -> "what",
        "null_check" -> null,
        "nested" -> Map("who" -> "me")
      )
    )
    AlertEventProcessor.logAlert(event)

    // Remove all service fields (logger and thread related and etc.)
    val serviceFields = Seq("endOfBatch", "level", "loggerName", "loggerFqcn", "timeMillis", "thread", "threadId", "threadPriority")
    val generatedEvent = Jackson.parse[Dict](writer.toString) -- serviceFields

    generatedEvent must be(Map(
      "message" -> "describe me",
      "contextMap" ->  Map(
        "timestamp" -> "2016-09-23T00:00:00Z",
        "hey" -> "what",
        "host" -> "127.0.0.1",
        "nested" -> "Map(who -> me)",
        "service" -> "health-test",
        "severity" -> "anomaly",
        "null_check" -> "null"
      )
    ))
  }
}
