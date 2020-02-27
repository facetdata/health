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

package com.metamx.health.hq

import com.metamx.common.scala.Predef._
import com.metamx.common.scala.db.DBConfig
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.health.core.config.HealthRainerConfig
import java.util.Properties
import org.joda.time.{Duration, Period}
import org.skife.config.{Config, Default}
import scala.collection.JavaConverters._

object config
{
  abstract class HealthHQConfig
  {
    @Config(Array("health.hq.processor.topic.statuses"))
    def statusesTopic: String

    @Config(Array("health.hq.processor.topic.pagers"))
    def pagersTopic: String

    @Config(Array("health.hq.processor.topic.alerts"))
    def alertsTopic: String

    @Config(Array("health.hq.processor.topic.heartbeats"))
    def heartbeatsTopic: String

    @Config(Array("health.hq.processor.notify.severity"))
    @Default("service-failure")
    def notifySeverity: Severity

    @Config(Array("health.hq.status.historySize"))
    @Default("10")
    def statusHistorySize: Int

    lazy val props = { assert(_props != null); _props }
    var _props = null : Properties

    def init(props: Properties): HealthHQConfig = {
      _props  = props
      this
    }

    def processorKafkaProps = {
      val prefix = "health.hq.processor.kafka."
      new Properties() withEffect {
        x => x.putAll(props.asScala.filterKeys(_.startsWith(prefix)).map(x => (x._1.stripPrefix(prefix), x._2)).asJava)
      }
    }
  }

  trait HealthStatusDBConfig extends DBConfig
  {
    @Config(Array("health.config.status.db.uri"))
    def uri: String

    @Config(Array("health.config.status.db.user"))
    def user: String

    @Config(Array("health.config.status.db.password"))
    def password: String

    @Config(Array("health.config.status.db.queryTimeout"))
    @Default("PT60S")
    def queryTimeout: Duration

    @Config(Array("health.config.status.db.batchSize"))
    @Default("1000")
    def batchSize: Int

    @Config(Array("health.config.status.db.fetchSize"))
    @Default("1000")
    def fetchSize: Int
  }

  trait HealthRainerHeartbeatConfig extends HealthRainerConfig
  {
    @Config(Array("health.config.heartbeat.table"))
    def table: String

    @Config(Array("health.config.heartbeat.zkPath"))
    def zkPath: String
  }

  trait HealthHeartbeatConfig
  {
    @Config(Array("health.heartbeat.pagerKey"))
    def pagerKey: String

    @Config(Array("health.heartbeat.rainerConfigKey"))
    @Default("config")
    def rainerConfigKey: String

    @Config(Array("health.heartbeat.watchdogPeriod"))
    @Default("PT1S")
    def watchdogPeriod: Period

    @Config(Array("health.heartbeat.unexpectedAgentAlertPeriod"))
    @Default("PT15M")
    def unexpectedAgentAlertPeriod: Period
  }
}
