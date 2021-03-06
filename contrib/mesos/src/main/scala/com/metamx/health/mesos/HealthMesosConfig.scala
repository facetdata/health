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

package com.metamx.health.mesos

import com.metamx.health.core.config.HealthRainerConfig
import org.joda.time.Period
import org.skife.config.{Config, Default}

abstract class HealthMesosConfig extends HealthRainerConfig
{
  @Config(Array("health.mesos.table"))
  override def table: String

  @Config(Array("health.mesos.zkPath"))
  override def zkPath: String

  @Config(Array("health.mesos.period"))
  @Default("PT5M")
  def period: Period

  @Config(Array("health.mesos.heartbeatPeriod"))
  @Default("PT15S")
  def heartbeatPeriod: Period

  @Config(Array("health.mesos.probe.total.timeout"))
  @Default("PT10s")
  def probeTotalTimeout: Period

  @Config(Array("health.mesos.master.fetch.timeout"))
  @Default("PT10s")
  def masterFetchTimeout: Period

  @Config(Array("health.mesos.close.timeout"))
  @Default("PT10s")
  def closeTimeout: Period
}
