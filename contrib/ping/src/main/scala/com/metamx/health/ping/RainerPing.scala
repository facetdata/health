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

import com.metamx.common.IAE
import com.metamx.common.scala.net.uri._
import com.metamx.common.scala.untyped._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Yaml
import com.metamx.health.agent.AgentDiscoConfig
import com.metamx.rainer.Commit
import com.metamx.rainer.KeyValueDeserialization
import java.io.ByteArrayInputStream
import org.joda.time.Period

/**
 * Configuration for PingProbe.
 *
 * @param group Some name for the group of services monitored by this probe.
 * @param services Map of service name -> monitored service.
 */
case class RainerPing(
  group: String,
  services: Map[String, PingableService]
)

case class PingResponse(
  statusCode: Int,
  body: String
)

case class PingResult(
  ok: Boolean,
  data: Dict = Map.empty
)

/**
 * One service monitored by the PingProbe.
 * @param name Descriptive name of the service.
 * @param uri URI of the service.
 * @param postData Data to be POSTed to the URI
 * @param predicate Predicate to process the response
 * @param pingPeriod Custom ping period for the service
 * @param failCount How many pings should fail in a row before reporting a failure.
 * @param resolveCount How many pings should succeed in a row before resolving a failure.
 * @param ignoreFailedFetch Ignore the failures due to timeout (failed-fetch)
 */
case class PingableService(
  name: String,
  uri: URI,
  discoConfig: Option[AgentDiscoConfig],
  postData: Option[() => String],
  pagerKey: Option[String],
  predicate: PingResponse => PingResult,
  pingPeriod: Option[Period],
  failCount: Int,
  resolveCount: Int,
  ignoreFailedFetch: Boolean,
  definition: Dict
)

object RainerPing extends Logging
{
  implicit val deserialization = new KeyValueDeserialization[RainerPing]
  {
    override def fromKeyAndBytes(k: Commit.Key, bytes: Array[Byte]) = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))
      val services = dict(d("services"))
      RainerPing(
        k,
        for ((serviceName, serviceDict) <- services.mapValues(dict(_))) yield {
          def parse[A](dictKey: String, f: Option[Any] => A): A = {
            try f(serviceDict.get(dictKey))
            catch {
              case e: Exception =>
                throw new IllegalArgumentException("Invalid '%s' for service: %s" format(dictKey, serviceName), e)
            }
          }

          val uri = parse("uri", o => new URI(str(o.get)))

          val discoConfigOpt = (
            serviceDict.get("zkConnect").map(_.toString),
            serviceDict.get("discoPath").map(_.toString)
          ) match {
            case (Some(zkConnectStr), Some(discoPathStr)) => Some(
              new AgentDiscoConfig {
                override val zkConnect: String = zkConnectStr
                override val discoPath: String = discoPathStr
              }
            )

            case (None, None) => None

            case _ => throw new IAE("zkConnect and discoPath should be either both defined or both undefined")
          }

          val postData = parse("postData", o => o.map(PostDataFunction.fromAny))
          val pagerKey = parse("pagerKey", o => o.map(str(_)))
          val predicate = parse("predicate", o => o.map(PingResponsePredicate.fromAny)) getOrElse {
            PingResponsePredicate.default
          }
          require(predicate != null, "WTF?! Why is the predicate null?")

          val pingPeriod = parse("pingPeriod", o => o.map(x => new Period(x)))
          val failCount = parse("failCount", o => int(o.getOrElse(3)))
          val resolveCount = parse("resolveCount", o => int(o.getOrElse(2)))
          val ignoreFailedFetch = parse("ignoreFailedFetch", o => bool(o.getOrElse(false)))

          (
            serviceName,
            PingableService(
              serviceName,
              uri,
              discoConfigOpt,
              postData,
              pagerKey,
              predicate,
              pingPeriod,
              failCount,
              resolveCount,
              ignoreFailedFetch,
              serviceDict
            )
          )
        }
      )
    }
  }
}
