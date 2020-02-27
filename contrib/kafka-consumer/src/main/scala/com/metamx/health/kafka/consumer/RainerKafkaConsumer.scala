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

package com.metamx.health.kafka.consumer

import com.metamx.common.scala.Yaml
import com.metamx.common.scala.untyped._
import com.metamx.health.kafka.TopicThreshold
import com.metamx.rainer.{Commit, KeyValueDeserialization}
import java.io.ByteArrayInputStream
import org.joda.time.Period

/**
 * Configuration for KafkaConsumerAgent, scoped to a single Kafka environment.
 *
 * @param zkPath Base ZK path for this kafka environment.
 * @param thresholdAlert Map of (consumerGroup, topic) -> maximum offset delta.
 */
case class RainerKafkaConsumer(
  zkPath: String,
  pagerKey: Option[String],
  thresholdAlert: Map[(String, String), TopicThreshold]
)

object RainerKafkaConsumer
{
  implicit val deserialization = new KeyValueDeserialization[RainerKafkaConsumer] {
    override def fromKeyAndBytes(k: Commit.Key, bytes: Array[Byte]) = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))
      def thresholds(k: String) = {
        for {
          (consumerGroup, topicOffsets) <- dict(d.getOrElse(k, Map.empty))
          (topic, payload) <- dict(topicOffsets)
        } yield {
          normalize(payload) match {
            case t: Dict =>
              ((consumerGroup, topic), TopicThreshold(
                Period.parse(str(t("alert"))),
                Period.parse(str(t("pager")))
              ))

            case t =>
              throw new ClassCastException(
                "Cannot interpret alert payload of class[%s]: %s".format(t.getClass.getName, t)
              )
          }
        }
      }
      RainerKafkaConsumer(
        str(d("path")),
        d.get("pagerKey").map(str(_)),
        thresholds("alert")
      )
    }
  }
}

