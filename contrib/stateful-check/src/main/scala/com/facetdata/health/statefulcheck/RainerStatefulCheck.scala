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

import com.metamx.common.scala.Logging
import com.metamx.common.scala.Yaml
import com.metamx.common.scala.untyped._
import com.metamx.rainer.Commit
import com.metamx.rainer.KeyValueDeserialization
import java.io.ByteArrayInputStream
import org.joda.time.Period

case class RainerStatefulCheck(
  group: String,
  checks: Map[String, StatefulCheck]
)

/**
  * Stateful check.
  *
  * @param name          Descriptive name of the service.
  * @param checkFunction Predicate to process the response
  * @param checkPeriod   Custom ping period for the service
  * @param failCount     How many pings should fail in a row before reporting a failure.
  * @param resolveCount  How many pings should succeed in a row before resolving a failure.
  * @param definition    Full definition of the check
  */
case class StatefulCheck(
  group: String,
  name: String,
  pagerKey: Option[String],
  checkFunction: CheckFunction.CheckFn,
  checkPeriod: Option[Period],
  failCount: Int,
  resolveCount: Int,
  definition: Dict,
  enabled: Boolean = true
)

object RainerStatefulCheck extends Logging
{
  implicit val deserialization: KeyValueDeserialization[RainerStatefulCheck] = new KeyValueDeserialization[RainerStatefulCheck]
  {
    override def fromKeyAndBytes(k: Commit.Key, bytes: Array[Byte]): RainerStatefulCheck = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))
      val checks = dict(d("checks"))
      RainerStatefulCheck(
        k,
        for ((checkName, checkDefinition) <- checks.mapValues(dict(_))) yield {
          def parse[A](dictKey: String, f: Option[Any] => A): A = {
            try f(checkDefinition.get(dictKey))
            catch {
              case e: Exception =>
                throw new IllegalArgumentException("Invalid '%s' for checks: %s" format(dictKey, checkName), e)
            }
          }

          val checkFunction =
            parse("check", o => o.map(str(_)).map(CheckFunction.fromString)
              .getOrElse(CheckFunction.default))
          require(checkFunction != null, "Check function is null")
          val checkPeriod = parse("checkPeriod", o => o.map(x => new Period(x)))
          val failCount = parse("failCount", o => int(o.getOrElse(3)))
          val resolveCount = parse("resolveCount", o => int(o.getOrElse(2)))
          val pagerKey = parse("pagerKey", o => o.map(str(_)))
          val enabled = parse("enabled", o => o.forall(bool(_)))

          (
            checkName,
            StatefulCheck(
              k,
              checkName,
              pagerKey,
              checkFunction,
              checkPeriod,
              failCount,
              resolveCount,
              checkDefinition,
              enabled
            )
          )
        }
      )
    }
  }
}
