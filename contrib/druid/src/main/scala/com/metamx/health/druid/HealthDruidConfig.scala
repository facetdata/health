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

import com.metamx.health.core.config.HealthRainerConfig
import org.joda.time.{Duration, Period}
import org.skife.config.{Config, Default}

abstract class HealthDruidConfig extends HealthRainerConfig
{
  @Config(Array("health.druid.table"))
  def table: String

  @Config(Array("health.druid.zkPath"))
  def zkPath: String

  @Config(Array("health.druid.period"))
  @Default("PT5M")
  def period: Period

  @Config(Array("health.druid.stale.data.alert.throttle"))
  @Default("PT1H")
  def staleDataAlertThrottle: Period

  @Config(Array("health.druid.loadstatus.alert.grace.period"))
  @Default("PT15M")
  def loadStatusGracePeriod: Period

  @Config(Array("health.druid.http.timeout"))
  @Default("PT10S")
  def httpTimeout: Duration

  @Config(Array("health.druid.heartbeatPeriod"))
  @Default("PT15S")
  def heartbeatPeriod: Period
}
