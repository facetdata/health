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
import com.metamx.common.scala.untyped._
import javax.script.Invocable

trait CheckFunction
{
  def apply(): Any
}

object CheckFunction
{
  type CheckFn = () => CheckResults

  private val DefaultPredicate: CheckFn = () => CheckResults(Array(CheckResult(name = "default")))

  def default: CheckFn = DefaultPredicate

  def fromString(s: String): CheckFn = apply(s)

  def apply(function: String): CheckFn =
  {
    val engine = JsScriptEngine.create()
    engine.eval("var apply = %s" format function)

    val predicate = engine.asInstanceOf[Invocable].getInterface(classOf[CheckFunction])
    () => tryCasts(predicate())(
      res =>
        res.asInstanceOf[CheckResults]
    )
  }
}
