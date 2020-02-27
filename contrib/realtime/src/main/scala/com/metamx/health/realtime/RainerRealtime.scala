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

package com.metamx.health.realtime

import com.metamx.common.scala.Yaml
import com.metamx.common.scala.untyped._
import com.metamx.health.agent.AgentDiscoConfig
import com.metamx.health.kafka.TopicThreshold
import com.metamx.rainer.Commit._
import com.metamx.rainer.KeyValueDeserialization
import java.io.ByteArrayInputStream
import org.joda.time.Period

case class RainerRealtime(
  zkConnect: String,
  discoPath: String,
  guardianService: String,
  pagerKey: Option[String],
  threshold: TopicThreshold
) extends AgentDiscoConfig

object RainerRealtime
{
  implicit val deserialize = new KeyValueDeserialization[RainerRealtime]
  {
    override def fromKeyAndBytes(k: Key, bytes: Array[Byte]): RainerRealtime = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))

      RainerRealtime(
        str(d("zkConnect")),
        str(d("discoPath")),
        str(d("guardianService")),
        d.get("pagerKey").map(str(_)),
        TopicThreshold(Period.parse(str(d("alertThreshold"))), Period.parse(str(d("pagerThreshold"))))
      )
    }
  }
}
