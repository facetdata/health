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

package com.metamx.health.druid

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Yaml
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.untyped._
import com.metamx.health.agent.AgentDiscoConfig
import com.metamx.rainer.Commit
import com.metamx.rainer.KeyValueDeserialization
import java.io.ByteArrayInputStream

/**
 * Configuration for DruidProbe, scoped to a single Druid environment.
 *
 * @param zkConnect Discovery's zookeeper connection string
 * @param discoPath Discovery path
 * @param broker Service discovery key for this environment's broker.
 * @param coordinator Service discovery key for this environment's coordinator.
 * @param thresholdAlert Map of dataSource -> alert payload.
 */
case class RainerDruid(
  zkConnect: String,
  discoPath: String,
  broker: String,
  coordinator: String,
  thresholdAlert: Map[String, RainerDruidAlertPayload]
) extends AgentDiscoConfig

case class RainerDruidAlertPayload(
  threshold: Period,
  pagerKey: Option[String]
)

object RainerDruid
{
  implicit val deserialization = new KeyValueDeserialization[RainerDruid]
  {
    override def fromKeyAndBytes(k: Commit.Key, bytes: Array[Byte]): RainerDruid = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))

      def alertPayloads(k: String): Map[String, RainerDruidAlertPayload] = {
        val alertPayloadsDict: Dict = dict(d.getOrElse(k, Map.empty))
        alertPayloadsDict strictMapValues { alertPayloadAny =>
          normalize(alertPayloadAny) match {
            case s: String => // Use string as threshold, no pager.
              RainerDruidAlertPayload(new Period(str(s)), None)
            case d: Dict =>
              val threshold = str(d("threshold"))
              val pagerKey = Option(d.getOrElse("pagerKey", null)).map(str(_))
              RainerDruidAlertPayload(new Period(threshold), pagerKey)
            case o =>
              throw new ClassCastException(
                "Cannot interpret alert payload of class[%s]: %s" format
                  (o.getClass.getName, o)
              )
          }
        }
      }

      RainerDruid(
        str(d("zkConnect")),
        str(d("discoPath")),
        str(d("broker")),
        str(d("coordinator")),
        alertPayloads("alert")
      )
    }
  }
}
