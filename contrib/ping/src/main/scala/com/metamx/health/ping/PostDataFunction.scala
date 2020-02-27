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

import com.metamx.common.scala.untyped._
import com.metamx.health.ping.PostDataFunction.PostDataFn
import javax.script.{Invocable, ScriptEngineManager}


/**
 * Simple trait to help out the JS scripting engine. It has trouble implementing Function0 directly.
 */
trait SimplePostDataFunction
{
  def apply(): String
}

object PostDataFunction
{
  type PostDataFn = () => String

  def fromString(s: String): PostDataFn = () => s

  def fromDict(d: Dict): PostDataFn = d.get("type") match {
    case Some("string") =>
      val theString = str(d.getOrElse("value", throw new IllegalArgumentException("Missing 'value'")))
      () => theString

    case Some("js") =>
      JsPostDataFunction(str(d.getOrElse("function", throw new IllegalArgumentException("Missing 'function'"))))

    case Some(x) =>
      throw new IllegalArgumentException("Unknown postData type: %s" format x)

    case None =>
      throw new IllegalArgumentException("Missing postData type")
  }

  def fromAny(o: Any): PostDataFn = tryCasts(o)(
    x => fromString(str(x)),
    x => fromDict(dict(x))
  )
}

object JsPostDataFunction
{
  def apply(function: String): PostDataFn =
  {
    val engine = JsScriptEngine.create()
    engine.eval("var apply = %s" format function)
    val simplePostDataFn = engine.asInstanceOf[Invocable].getInterface(classOf[SimplePostDataFunction])
    () => simplePostDataFn()
  }
}
