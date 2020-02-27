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

package com.metamx.health.core

import com.metamx.common.scala.db.DBConfig
import com.metamx.common.scala.net.curator.{CuratorConfig, DiscoAnnounceConfig, DiscoConfig}
import org.joda.time.{Duration, Period}
import org.skife.config.{Config, Default}

object config
{
  trait ServiceConfig
  {
    @Config(Array("com.metamx.service"))
    def service: String

    @Config(Array("com.metamx.host"))
    def host: String

    @Config(Array("com.metamx.port"))
    def port: Int

    @Config(Array("com.metamx.health.http.maxThreads"))
    @Default("10")
    def maxThreads: Int

    @Config(Array("com.metamx.emitter.timeout"))
    @Default("PT5M")
    def emitterTimeout: Period
  }

  abstract class HealthCuratorConfig extends CuratorConfig with DiscoConfig with ServiceConfig
  {
    @Config(Array("health.zk.connect"))
    override def zkConnect: String

    @Config(Array("health.zk.timeout"))
    @Default("PT15S")
    override def zkTimeout: Duration

    @Config(Array("health.zk.discoPath"))
    override def discoPath: String

    override def discoAnnounce = Some(DiscoAnnounceConfig(service.replace("/", ":"), port, false))
  }

  trait HealthDBConfig extends DBConfig
  {
    @Config(Array("health.config.db.uri"))
    def uri: String

    @Config(Array("health.config.db.user"))
    def user: String

    @Config(Array("health.config.db.password"))
    def password: String

    @Config(Array("health.config.db.queryTimeout"))
    @Default("PT60S")
    def queryTimeout: Duration

    @Config(Array("health.config.db.batchSize"))
    @Default("1000")
    def batchSize: Int

    @Config(Array("health.config.db.fetchSize"))
    @Default("1000")
    def fetchSize: Int
  }

  trait HealthRainerConfig
  {
    @Config(Array("health.config.zk.publishDuration"))
    @Default("PT300S")
    def autoPublishDuration: Duration

    @Config(Array("health.config.zk.publishFuzz"))
    @Default("0.2")
    def autoPublishFuzz: Double

    def table: String

    def zkPath: String
  }

  trait HealthRainerNotifyInventoryConfig extends HealthRainerConfig
  {
    @Config(Array("health.notify.inventory.key"))
    @Default("config")
    def key: String

    @Config(Array("health.notify.inventory.table"))
    def table: String

    @Config(Array("health.notify.inventory.zkPath"))
    def zkPath: String
  }
}
