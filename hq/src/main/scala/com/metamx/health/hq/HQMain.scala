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

import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.concurrent.abortingThread
import com.metamx.common.scala.config._
import com.metamx.common.scala.db.MySQLDB
import com.metamx.common.scala.lifecycle._
import com.metamx.common.scala.net.curator.Discotheque
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.core.AbstractMain
import com.metamx.health.core.config.HealthDBConfig
import com.metamx.health.core.config.HealthRainerNotifyInventoryConfig
import com.metamx.health.core.config.ServiceConfig
import com.metamx.health.hq.api.HqHealthServlet
import com.metamx.health.hq.api.StatusServlet
import com.metamx.health.hq.config.HealthHQConfig
import com.metamx.health.hq.config.HealthHeartbeatConfig
import com.metamx.health.hq.config.HealthRainerHeartbeatConfig
import com.metamx.health.hq.config.HealthStatusDBConfig
import com.metamx.health.hq.db.PostgreSQLDB
import com.metamx.health.hq.db.PostgreSqlStatusStorageMixin
import com.metamx.health.hq.processors._
import com.metamx.health.notify.inventory.NotifyInventoryConfig
import com.metamx.health.notify.inventory.RainerNotifyInventory
import com.metamx.health.rainer.RainerStack
import com.metamx.metrics.JvmMonitor
import com.metamx.metrics.Monitor
import com.metamx.metrics.SysMonitor
import com.metamx.rainer.DbCommitStorageMySQLMixin
import java.util.Properties
import javax.servlet.Servlet
import org.apache.curator.framework.CuratorFramework
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.skife.config.ConfigurationObjectFactory

// TODO: review all TODOs :)
// TODO: check the places where HQ might fail and setup proper pager notifications
object HQMain extends AbstractMain
{
  override protected def main(
    lifecycle: Lifecycle,
    configs: ConfigurationObjectFactory,
    props: Properties,
    emitter: ServiceEmitter,
    curator: CuratorFramework,
    discotheque: Discotheque
  ) {
    val serviceConfig = configs.apply[ServiceConfig]

    val healthHQConfig = configs.apply[HealthHQConfig].init(props)
    val configDb = lifecycle(new MySQLDB(configs.apply[HealthDBConfig]) with DbCommitStorageMySQLMixin)
    val statusDb = lifecycle(new PostgreSQLDB(configs.apply[HealthStatusDBConfig]) with PostgreSqlStatusStorageMixin)

    val rainerNotifyInventoryConfig = configs.apply[HealthRainerNotifyInventoryConfig]
    val notifyInventoryRainerStack = lifecycle(
      RainerStack.create[NotifyInventoryConfig](
        curator,
        configDb,
        rainerNotifyInventoryConfig,
        emitter
      )(RainerNotifyInventory.deserialization)
    )
    val notifyInventory = new RainerNotifyInventory(
      emitter,
      rainerNotifyInventoryConfig.key,
      notifyInventoryRainerStack.keeper,
      configs.apply[ServiceConfig]
    )

    val statusEventProcessor = new StatusEventProcessor(
      emitter,
      healthHQConfig.statusesTopic,
      healthHQConfig.processorKafkaProps,
      healthHQConfig.statusHistorySize,
      statusDb
    )

    val statusServlet = new StatusServlet(statusDb)

    val pagerEventProcessor = new PagerEventProcessor(
      notifyInventory,
      emitter,
      healthHQConfig.pagersTopic,
      healthHQConfig.processorKafkaProps
    )

    val alertEventProcessor = new AlertEventProcessor(
      notifyInventory,
      emitter,
      healthHQConfig.alertsTopic,
      healthHQConfig.processorKafkaProps,
      healthHQConfig.notifySeverity
    )

    val rainerHeartbeatConfig = configs.apply[HealthRainerHeartbeatConfig]
    val heartbeatRainerStack = lifecycle(
      RainerStack.create[HeartbeatConfig](
        curator,
        configDb,
        rainerHeartbeatConfig,
        emitter
      )(HeartbeatEventProcessor.deserialization)
    )

    val heartbeatConfig = configs.apply[HealthHeartbeatConfig]
    val heartbeatEventProcessor = new HeartbeatEventProcessor(
      heartbeatConfig,
      notifyInventory,
      heartbeatRainerStack.keeper,
      emitter,
      healthHQConfig.heartbeatsTopic,
      healthHQConfig.processorKafkaProps
    )

    val eventProcessorsMonitors: Seq[Monitor] =
      statusEventProcessor.monitors ++
      pagerEventProcessor.monitors ++
      alertEventProcessor.monitors ++
      heartbeatEventProcessor.monitors

    addMonitors(Seq(new SysMonitor, new JvmMonitor) ++ eventProcessorsMonitors)(lifecycle, configs, emitter)


    // HTTP
    initServletServer(
      serviceConfig.port,
      serviceConfig.maxThreads,
      lifecycle,
      "/health" -> new HqHealthServlet,
      "/config/notify-inventory/*" -> notifyInventoryRainerStack.servlet,
      "/config/heartbeat/*" -> heartbeatRainerStack.servlet,
      "/api/v1/status/*" -> statusServlet
    )


    lifecycle.start()
    statusEventProcessor.init()

    notifyInventory.init()
    heartbeatEventProcessor.init()

    abortingThread { statusEventProcessor.run() } start()
    abortingThread { pagerEventProcessor.run() } start()
    abortingThread { alertEventProcessor.run() } start()
    abortingThread { heartbeatEventProcessor.run() } start()

    lifecycle.join()
  }

  def initServletServer(port: Int, maxThreads: Int, lifecycle: Lifecycle, servlets: (String, Servlet)*) = {
    new Server(
      new QueuedThreadPool() withEffect { pool =>
        pool.setMinThreads(maxThreads)
        pool.setMaxThreads(maxThreads)
      }
    ) withEffect {
      srv =>
        srv.setConnectors(
          Array(
            new ServerConnector(srv) withEffect {
              _.setPort(port)
            }
          )
        )

        srv.setHandler(
          new ServletContextHandler(ServletContextHandler.SESSIONS) withEffect {
            ctxt => {
              ctxt.setContextPath("/")
              servlets foreach {
                case (path, servlet) =>
                  ctxt.addServlet(new ServletHolder(servlet), path)
              }
            }
          }
        )

        lifecycle onStart {
          srv.start()
        } onStop {
          srv.stop()
        }
    }
  }
}
