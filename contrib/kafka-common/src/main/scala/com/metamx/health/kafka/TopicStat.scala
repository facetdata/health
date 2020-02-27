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

package com.metamx.health.kafka

import com.metamx.common.scala.untyped.Dict
import com.metamx.health.util.PeriodUtil
import org.joda.time.Period

case class TopicStat(
  threshold: TopicThreshold,
  partitionsCount: Int,
  alertPartitionsCount: Int = 0,
  pagerPartitionsCount: Int = 0,
  maxLagPartition: Int = -1,
  maxLagDelta: PartitionDelta = PartitionDelta(0, 0),
  totalDelta: PartitionDelta = PartitionDelta(0, 0)
)
{
  def avgDelta = PartitionDelta(
    math.round(totalDelta.messages / partitionsCount.toDouble),
    math.round(totalDelta.time / partitionsCount.toDouble)
  )

  def add(partition: Int, partitionDelta: PartitionDelta): TopicStat = {
    val alertCount = if (partitionDelta.time > threshold.alertThresholdMillis) 1 else 0
    val pagerCount = if (partitionDelta.time > threshold.pagerThresholdMillis) 1 else 0

    if (partitionDelta.time > maxLagDelta.time) {
      copy(
        totalDelta = this.totalDelta + partitionDelta,
        alertPartitionsCount = this.alertPartitionsCount + alertCount,
        pagerPartitionsCount = this.pagerPartitionsCount + pagerCount,
        maxLagPartition = partition,
        maxLagDelta = partitionDelta
      )
    } else {
      copy(
        totalDelta = this.totalDelta + partitionDelta,
        alertPartitionsCount = this.alertPartitionsCount + alertCount,
        pagerPartitionsCount = this.pagerPartitionsCount + pagerCount
      )
    }
  }

  private def lagDetails(lagPartitionsCount: Int, thresholdPeriod: Period) = {
    if (lagPartitionsCount > 0) {
      Some(
        Map(
          "partition" -> maxLagPartition,
          "info" -> Map(
            "lagPartitionsCount" -> lagPartitionsCount,
            "messages" -> Map(
              "totalDelta" -> "%,d msg".format(totalDelta.messages),
              "avgDelta" -> "%,d msg".format(avgDelta.messages),
              "maxPartitionDelta" -> "%,d msg".format(maxLagDelta.messages)
            ),
            "time" -> Map(
              "avgDelta" -> PeriodUtil.format(new Period(avgDelta.time)),
              "maxPartitionDelta" -> PeriodUtil.format(new Period(maxLagDelta.time))
            )
          ),
          "health" -> Map(
            "threshold" -> PeriodUtil.format(thresholdPeriod),
            "delta" -> PeriodUtil.format(new Period(maxLagDelta.time))
          )
        )
      )
    } else {
      None
    }
  }

  def alertMap: Option[Dict] = lagDetails(alertPartitionsCount, threshold.alertThreshold)
  def pagerMap: Option[Dict] = lagDetails(pagerPartitionsCount, threshold.pagerThreshold)
}

case class TopicThreshold(alertThreshold: Period, pagerThreshold: Period)
{
  val alertThresholdMillis = alertThreshold.toStandardDuration.getMillis
  val pagerThresholdMillis = pagerThreshold.toStandardDuration.getMillis

  override def toString = "alert threshold: %,d ms, pager threshold: %,d ms"
    .format(alertThresholdMillis, pagerThresholdMillis)
}
