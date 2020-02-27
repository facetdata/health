package com.metamx.health.test

import com.github.nscala_time.time.Imports._
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.metamx.health.ping.PingResponse
import com.metamx.health.ping.PingResult
import com.metamx.health.ping.RainerPing
import java.net.URI
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.MustMatchers

@RunWith(classOf[JUnitRunner])
class PingTest extends FunSuite with MustMatchers
{
  test("JsPredicate") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    predicate: 'function(response) { try { return JSON.parse(response.body()).hey == "what" && response.statusCode() == 200; } catch(e) { return false; } }'
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.predicate.apply(PingResponse(200, """{"hey":"what"}""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """{"hey":"what"}""")).ok must be(false)
    service.predicate.apply(PingResponse(200, """{"hey":"no"}""")).ok must be(false)
    service.predicate.apply(PingResponse(201, """{"hey":"no"}""")).ok must be(false)
    service.predicate.apply(PingResponse(200, """what""")).ok must be(false)
    service.predicate.apply(PingResponse(201, """what""")).ok must be(false)
    service.definition must be(
      Map(
        "uri" -> "localhost:9119",
        "predicate" ->
          """function(response) { try { return JSON.parse(response.body()).hey == "what" && response.statusCode() == 200; } catch(e) { return false; } }"""
      )
    )
  }

  test("JsPredicateFromDict") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    predicate:
        |      type: js
        |      function: 'function(response) { try { return JSON.parse(response.body()).hey == "what" && response.statusCode() == 200; } catch(e) { return false; } }'
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.predicate.apply(PingResponse(200, """{"hey":"what"}""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """{"hey":"what"}""")).ok must be(false)
    service.predicate.apply(PingResponse(200, """{"hey":"no"}""")).ok must be(false)
    service.predicate.apply(PingResponse(201, """{"hey":"no"}""")).ok must be(false)
    service.predicate.apply(PingResponse(200, """what""")).ok must be(false)
    service.predicate.apply(PingResponse(201, """what""")).ok must be(false)
  }

  test("LoadExternalJs") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    predicate:
        |      type: js
        |      function: |
        |       function(response) {
        |         try {
        |           ExtJs.load("/test-utilities.js"); // check test resources
        |           data = JSON.parse(response.body())
        |           return PingResult.create(true, {"multiply" : multiply(data.a, data.b)});
        |         }
        |         catch(e) { return e; }
        |       }
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.predicate.apply(PingResponse(200, """{"a": 3.0,"b": 2.0}""")) must be(
      PingResult(true, Map("multiply" -> 6.0))
    )
  }

  test("ComplexJsPredicate") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    predicate:
        |      type: js
        |      function: |
        |       function(response)
        |       {
        |         try {
        |           var resp = JSON.parse(response.body());
        |           var violators = new Array();
        |           for (i in resp) {
        |             Log.warn("Host: %s, Value: %s", resp[i].host, resp[i].value);
        |             if (resp[i].value > 50) violators.push(resp[i].host);
        |           }
        |           var ok = response.statusCode() == 200 && violators.length > 0;
        |           return PingResult.create(ok, { "count" : resp.length, "violators" : violators.toString() } );
        |         }
        |         catch(e) { return false; }
        |       }
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.predicate.apply(
      PingResponse(
        200,
        """[{"value": 100.0,"host": "host1"},
          |{"value": 51.0,"host": "host2"},
          |{"value": 2.0,"host": "notMe"}]""".stripMargin
      )
    ) must be(
      PingResult(
        true,
        Map(
          "count" -> 3.0,
          "violators" -> "host1,host2"
        )
      )
    )
  }

  test("BadJsPredicate") {
    val e = intercept[IllegalArgumentException] {
      RainerPing.deserialization.fromKeyAndBytes(
        "foo",
        """
          |services:
          |  serviceName:
          |    uri: localhost:9119
          |    predicate: ';'
        """.stripMargin.getBytes(Charsets.UTF_8)
      )
    }

    assert(e.getMessage === """Invalid 'predicate' for service: serviceName""")
  }

  test("PredicateWithDate") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    predicate: 'function(response) { return new Date().getTime() > 10000 }'
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.predicate.apply(PingResponse(200, """{"hey":"what"}""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """{"hey":"what"}""")).ok must be(true)
    service.predicate.apply(PingResponse(200, """{"hey":"no"}""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """{"hey":"no"}""")).ok must be(true)
    service.predicate.apply(PingResponse(200, """what""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """what""")).ok must be(true)
  }

  test("PredicateWithISOParsing") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    predicate: 'function(response) { return DateTime.parse("2000-01-01T01:02:03Z") == 946688523000 }'
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.predicate.apply(PingResponse(200, """{"hey":"what"}""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """{"hey":"what"}""")).ok must be(true)
    service.predicate.apply(PingResponse(200, """{"hey":"no"}""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """{"hey":"no"}""")).ok must be(true)
    service.predicate.apply(PingResponse(200, """what""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """what""")).ok must be(true)
  }

  test("GmailRssPredicate") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    predicate:
        |      type: gmailRss
        |      senderAddress: alerts@example.com
        |      threshold: PT2H
        |      titleRegex: '.*HealthCheck.*'
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    val now = DateTime.now(DateTimeZone.UTC)
    val feed = new String(ByteStreams.toByteArray(getClass.getResource("/mail.rss").openStream()), Charsets.UTF_8)
    // Got mail recently
    service.predicate.apply(PingResponse(200, feed.format(now))) must be(
      PingResult(
        true,
        Map(
          "total mails" -> 3,
          "matching mails" -> 1,
          "recent mails" -> 1,
          "newest matching mail" -> now.toString()
        )
      )
    )
    service.predicate.apply(PingResponse(200, feed.format(now - 1.hour))) must be(
      PingResult(
        true,
        Map(
          "total mails" -> 3,
          "matching mails" -> 1,
          "recent mails" -> 1,
          "newest matching mail" -> (now - 1.hour).toString()
        )
      )
    )
    service.predicate.apply(PingResponse(200, feed.format(now - 2.hour + 1.minute))) must be(
      PingResult(
        true,
        Map(
          "total mails" -> 3,
          "matching mails" -> 1,
          "recent mails" -> 1,
          "newest matching mail" -> (now - 2.hour + 1.minute).toString()
        )
      )
    )
    service.predicate.apply(PingResponse(200, feed.format(now - 2.hour))) must be(
      PingResult(
        false,
        Map(
          "total mails" -> 3,
          "matching mails" -> 1,
          "recent mails" -> 0,
          "newest matching mail" -> (now - 2.hour).toString()
        )
      )
    )
  }

  test("BadGmailRssPredicate") {
    val e = intercept[IllegalArgumentException] {
      RainerPing.deserialization.fromKeyAndBytes(
        "foo",
        """
          |services:
          |  serviceName:
          |    uri: localhost:9119
          |    predicate:
          |      type: gmailRss
        """.stripMargin.getBytes(Charsets.UTF_8)
      )
    }

    assert(e.getMessage === """Invalid 'predicate' for service: serviceName""")
  }

  test("DefaultPredicate") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.predicate.apply(PingResponse(200, """{"hey":"what"}""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """{"hey":"what"}""")).ok must be(false)
    service.predicate.apply(PingResponse(200, """{"hey":"no"}""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """{"hey":"no"}""")).ok must be(false)
    service.predicate.apply(PingResponse(200, """what""")).ok must be(true)
    service.predicate.apply(PingResponse(201, """what""")).ok must be(false)
  }

  test("PostDataString") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    postData: 'hello world'
        |
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.postData.get.apply() must be("hello world")
  }

  test("PostDataStringTwo") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    postData:
        |     type: string
        |     value: 'hello world'
        |
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.postData.get.apply() must be("hello world")
  }

  test("PostDataJs") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    postData:
        |     type: js
        |     function: 'function(){ return new Date(0).toString().substr(0,10) }'
        |
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.postData.get.apply() must be("Thu Jan 01")
  }

  test("PostDataGenerateInterval") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    postData:
        |     type: js
        |     function: |
        |       function() {
        |         var now = new org.joda.time.DateTime(10000, org.joda.time.DateTimeZone.UTC);
        |         var then = now.minus(1000);
        |         return new org.joda.time.Interval(then, now).toString();
        |       }
        |
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.postData.get.apply() must be("1970-01-01T00:00:09.000Z/1970-01-01T00:00:10.000Z")
  }

  test("PostDataExtJs") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    postData:
        |     type: js
        |     function: |
        |       function(){
        |         ExtJs.load("/test-utilities.js")
        |         return "value=" + multiply(6, 8)
        |        }
        |
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.postData.get.apply() must be("value=48")
  }

  test("IgnoreFailedFetch") {
    val pinger = RainerPing.deserialization.fromKeyAndBytes(
      "foo",
      """
        |services:
        |  serviceName:
        |    uri: localhost:9119
        |    ignoreFailedFetch: True
      """.stripMargin.getBytes(Charsets.UTF_8)
    )
    val service = pinger.services("serviceName")
    service.name must be("serviceName")
    service.uri must be(new URI("localhost:9119"))
    service.ignoreFailedFetch must be(true)

    intercept[IllegalArgumentException]{
      val failPinger = RainerPing.deserialization.fromKeyAndBytes(
        "foo",
        """
          |services:
          |  serviceName:
          |    uri: localhost:9119
          |    ignoreFailedFetch: 1
        """.stripMargin.getBytes(Charsets.UTF_8)
      )
    }
  }
}
