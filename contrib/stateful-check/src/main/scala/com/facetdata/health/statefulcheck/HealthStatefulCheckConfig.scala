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

import com.metamx.health.core.config.HealthRainerConfig
import org.joda.time.Period
import org.skife.config.Config
import org.skife.config.Default

abstract class HealthStatefulCheckConfig extends HealthRainerConfig
{
  @Config(Array("health.statefulCheck.table"))
  def table: String

  @Config(Array("health.statefulCheck.zkPath"))
  def zkPath: String

  @Config(Array("health.statefulCheck.period"))
  @Default("PT1M")
  def period: Period

  @Config(Array("health.statefulCheck.heartbeatPeriod"))
  @Default("PT15S")
  def heartbeatPeriod: Period

  @Config(Array("health.statefulCheck.timeout"))
  @Default("PT15S")
  def timeout: Period
}
