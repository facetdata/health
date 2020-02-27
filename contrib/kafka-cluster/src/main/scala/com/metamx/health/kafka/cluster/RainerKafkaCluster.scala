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

package com.metamx.health.kafka.cluster

import com.metamx.common.scala.Yaml
import com.metamx.common.scala.untyped._
import com.metamx.rainer.Commit.Key
import com.metamx.rainer.KeyValueDeserialization
import java.io.ByteArrayInputStream
import org.joda.time.{Duration, Period}

case class RainerKafkaCluster(
  zkConnect: String,
  zkTimeout: Duration,
  brokersCount: Int,
  pagerKey: Option[String],
  jmxPort: Option[Int],
  brokerStartId: Int,
  thresholds: KafkaThresholds
)

case class KafkaThresholds(
  underReplicatedPartitionsPercentage: Option[Double],
  underReplicatedPeriod: Option[Period],
  offlinePartitions: Option[Boolean],
  uncleanLeaderElection: Option[Boolean]
)

object RainerKafkaCluster
{
  implicit val deserialize = new KeyValueDeserialization[RainerKafkaCluster] {
    override def fromKeyAndBytes(k: Key, bytes: Array[Byte]): RainerKafkaCluster = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))
      def thresholds(thresholds: Map[String, Any]) = {
        KafkaThresholds(
          thresholds.get("UnderReplicatedPartitionsPercentage").map(double(_)),
          thresholds.get("UnderReplicatedPeriod").map(new Period(_)),
          thresholds.get("OfflinePartitions").map(bool(_)),
          thresholds.get("UncleanLeaderElection").map(bool(_))
        )
      }

      RainerKafkaCluster(
        str(d("zkConnect")),
        d.get("zkTimeout").map(new Duration(_)).getOrElse(Duration.millis(30000)),
        int(d("brokersCount")),
        d.get("pagerKey").map(str(_)),
        d.get("jmxPort").map(int(_)),
        d.get("brokerStartId").map(int(_)).getOrElse(1),
        thresholds(dict(d.getOrElse("alert", Map.empty)))
      )
    }
  }
}
