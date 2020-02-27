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

import com.metamx.health.core.config.HealthRainerConfig
import com.metamx.health.kafka.KafkaProperties
import org.joda.time.Period
import org.skife.config.{Config, Default}

abstract class HealthKafkaConsumerConfig extends HealthRainerConfig
{
  @Config(Array("health.kafka.consumer.table"))
  def table: String

  @Config(Array("health.kafka.consumer.zkPath"))
  def zkPath: String

  @Config(Array("health.kafka.consumer.period"))
  @Default("PT15M")
  def period: Period

  @Config(Array("health.kafka.consumer.fetch.soTimeout"))
  @Default("30000")
  def soTimeout: Int

  @Config(Array("health.kafka.consumer.fetch.bufferSize"))
  @Default("1048576")
  def bufferSize: Int

  @Config(Array("health.kafka.consumer.fetch.fetchSize"))
  @Default("1048576")
  def fetchSize: Int

  @Config(Array("health.kafka.consumer.fetch.maxWait"))
  @Default("500")
  def maxWait: Int

  @Config(Array("health.kafka.consumer.kafkaPoolSize"))
  @Default("96")
  def kafkaPoolSize: Int

  @Config(Array("health.kafka.consumer.heartbeatPeriod"))
  @Default("PT15S")
  def heartbeatPeriod: Period

  def toKafkaProperties = {
    KafkaProperties(soTimeout, bufferSize, fetchSize, maxWait)
  }
}
