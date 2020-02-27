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

package com.metamx.health.ping

import com.metamx.common.logger.Logger
import com.metamx.common.scala.untyped._
import java.io.InputStreamReader
import javax.script.{ScriptEngine, ScriptEngineManager}
import org.joda.time.DateTime

object JsScriptEngine
{
  def create(): ScriptEngine = {
    val engine = new ScriptEngineManager().getEngineByMimeType("application/javascript")

    val dateTime = new {
      def parse(x: String): Long = new DateTime(x).getMillis

      def parse(x: Long): Long = new DateTime(x).getMillis
    }

    val pingResult = new {
      def create(ok: Boolean, data: Any = {}): PingResult = PingResult(ok, dict(data))
    }

    val extJs = new {
      def load(fileName: String) = {
        engine.eval(new InputStreamReader(getClass().getResourceAsStream(fileName)))
      }
    }

    engine.put("DateTime", dateTime)
    engine.put("PingResult", pingResult)
    engine.put("ExtJs", extJs)
    engine.put("Log", new Logger("JsPredicate"))
    engine
  }
}
