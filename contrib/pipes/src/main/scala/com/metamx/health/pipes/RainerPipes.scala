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

package com.metamx.health.pipes

import com.metamx.common.scala.Yaml
import com.metamx.common.scala.untyped.dict
import com.metamx.common.scala.untyped.str
import com.metamx.health.agent.AgentDiscoConfig
import com.metamx.rainer.Commit
import com.metamx.rainer.KeyValueDeserialization
import java.io.ByteArrayInputStream
import org.joda.time.Period

/**
 * Configuration for PipesProbe, scoped to a single Pipes environment. In general, zero-length pagerPeriods
 * mean that alerts are disabled.
 *
 * @param zkConnect Discovery's zookeeper connection string
 * @param discoPath Discovery path
 * @param control Service discovery key for this environment's control server.
 * @param delayPagerKeyOpt Pager service key for backlogged pipelines.
 * @param defaultDelayPagerPeriod Default maximum backlog lag. Applies to pipes with the default channel.
 * @param specificDelayPagerPeriods Map of pipe -> maximum backlog lag.
 * @param failurePagerKeyOpt Pager service key for failed pipes jobs.
 * @param defaultFailurePagerThrottle Default throttle period of failed jobs pager.
 * @param specificFailurePagerThrottles Map of pipe -> throttle period.
 */
case class RainerPipes(
  zkConnect: String,
  discoPath: String,
  control: String,
  delayPagerKeyOpt: Option[String],
  defaultDelayPagerPeriod: Period,
  specificDelayPagerPeriods: Map[String, Period],
  failurePagerKeyOpt: Option[String],
  defaultFailurePagerThrottle: Period,
  specificFailurePagerThrottles: Map[String, Period]
) extends AgentDiscoConfig

object RainerPipes
{
  implicit val deserialization = new KeyValueDeserialization[RainerPipes] {
    override def fromKeyAndBytes(k: Commit.Key, bytes: Array[Byte]) = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))
      def thresholds(k: String) = {
        dict(d.getOrElse(k, Map.empty)).map(kv => (kv._1, new Period(kv._2)))
      }
      RainerPipes(
        str(d("zkConnect")),
        str(d("discoPath")),
        str(d("control")),
        d.get("delayPagerKey").map(k => str(k)),
        new Period(d.getOrElse("defaultDelayPagerPeriod", "P4D")),
        thresholds("delayPagers"),
        d.get("failurePagerKey").map(k => str(k)),
        new Period(d.getOrElse("defaultFailurePagerThrottle", "PT6H")),
        thresholds("failurePagers")
      )
    }
  }
}
