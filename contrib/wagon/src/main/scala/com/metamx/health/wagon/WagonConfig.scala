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

package com.metamx.health.wagon

import org.skife.config.{Config, Default}

abstract class WagonConfig
{
  @Config(Array("health.wagon.pagerKey"))
  def pagerKey: String

  @Config(Array("health.ping.enable"))
  @Default("true")
  def pingOn: Boolean

  @Config(Array("health.druid.enable"))
  @Default("true")
  def druidOn: Boolean

  @Config(Array("health.pipes.enable"))
  @Default("true")
  def pipesOn: Boolean

  @Config(Array("health.kafka.consumer.enable"))
  @Default("true")
  def kafkaConsumerOn: Boolean

  @Config(Array("health.kafka.cluster.enable"))
  @Default("true")
  def kafkaClusterOn: Boolean

  @Config(Array("health.mesos.enable"))
  @Default("true")
  def mesosOn: Boolean

  @Config(Array("health.realtime.enable"))
  @Default("true")
  def realtimeOn: Boolean

  @Config(Array("health.statefulCheck.enable"))
  @Default("true")
  def statefulCheckOn: Boolean
}
