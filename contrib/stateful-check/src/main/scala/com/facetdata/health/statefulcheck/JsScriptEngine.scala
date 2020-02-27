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
import com.metamx.common.logger.Logger
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.untyped._
import java.io.InputStreamReader
import javax.script.ScriptEngine
import javax.script.ScriptEngineManager
import org.apache.http.client.fluent.Request
import org.apache.http.client.fluent.Response
import org.apache.http.entity.ContentType
import org.apache.http.util.EntityUtils
import org.joda.time.DateTime


object JsScriptEngine
{
  def create(): ScriptEngine = {
    val engine = new ScriptEngineManager().getEngineByMimeType("application/javascript")

    val dateTime = new {
      def parse(x: String): Long = new DateTime(x).getMillis

      def parse(x: Long): Long = new DateTime(x).getMillis
    }

    val extJs = new {
      def load(fileName: String): AnyRef = {
        engine.eval(new InputStreamReader(getClass.getResourceAsStream(fileName)))
      }
    }

    val checkResult = new {
      def create(name: String, ok: Boolean, data: Any = {}): CheckResult = CheckResult(name, ok, dict(data))
    }

    val checkResults = new {
      def create(results: Array[CheckResult]): CheckResults = CheckResults(results)
    }

    val mapper = Jackson.newObjectMapper()

    val httpClient = new {
      def get(address: String, headers: Any = {}): HttpClientResponse = {
        val request = {
          val request = Request.Get(address)
          dict(headers).foreach { case (headerName, headerValue) =>
            request.addHeader(headerName, headerValue.toString)
          }
          request
        }
        val response = request.execute().returnResponse()
        val content = EntityUtils.toString(response.getEntity)
        val statusLine = response.getStatusLine
        HttpClientResponse(
          statusLine.getStatusCode,
          statusLine.getReasonPhrase,
          content
        )
      }

      def post(address: String, headers: Any = {}, json: Any = {}): HttpClientResponse = {
        val request = {
          val req = Request.Post(address)
          dict(headers).foreach { case (headerName, headerValue) =>
            req.addHeader(headerName, headerValue.toString)
          }
          val body = tryCasts(json)(
            x => mapper.writeValueAsString(dict(x)),
            x => str(x)
          )
          if (!body.isEmpty) {
            req.bodyString(body, ContentType.APPLICATION_JSON)
          }
          req
        }
        val response = request.execute().returnResponse()
        val content = EntityUtils.toString(response.getEntity)
        val statusLine = response.getStatusLine
        HttpClientResponse(
          statusLine.getStatusCode,
          statusLine.getReasonPhrase,
          content
        )
      }
    }

    val envVar = new {
      def get(name: String): String = {
        System.getenv(name)
      }
    }

    engine.put("DateTime", dateTime)
    engine.put("ExtJs", extJs)
    engine.put("Log", new Logger("JsScriptEngine"))
    engine.put("HttpClient", httpClient)
    engine.put("CheckResult", checkResult)
    engine.put("CheckResults", checkResults)
    engine.put("Env", envVar)
    engine
  }
}

case class HttpClientResponse(
  status: Int,
  reason: String,
  content: String
) {
  def apply(response: Response): HttpClientResponse = {
    HttpClientResponse(
      response.returnResponse().getStatusLine.getStatusCode,
      response.returnResponse().getStatusLine.getReasonPhrase,
      response.returnContent().asString()
    )
  }
}
