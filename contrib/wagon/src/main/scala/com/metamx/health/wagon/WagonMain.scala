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

import com.facetdata.health.statefulcheck.HealthStatefulCheckConfig
import com.facetdata.health.statefulcheck.RainerStatefulCheck
import com.facetdata.health.statefulcheck.StatefulCheckAgent
import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.config._
import com.metamx.common.scala.db.MySQLDB
import com.metamx.common.scala.lifecycle._
import com.metamx.common.scala.net.curator.Discotheque
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.AlertAggregatorAgent
import com.metamx.health.agent.PeriodicAgent
import com.metamx.health.agent.PeriodicAgentsScheduler
import com.metamx.health.core.AbstractMain
import com.metamx.health.core.config.HealthCuratorConfig
import com.metamx.health.core.config.HealthDBConfig
import com.metamx.health.core.config.ServiceConfig
import com.metamx.health.druid.DruidAgent
import com.metamx.health.druid.HealthDruidConfig
import com.metamx.health.druid.RainerDruid
import com.metamx.health.kafka.cluster.HealthKafkaClusterConfig
import com.metamx.health.kafka.cluster.KafkaClusterAgent
import com.metamx.health.kafka.cluster.RainerKafkaCluster
import com.metamx.health.kafka.consumer.HealthKafkaConsumerConfig
import com.metamx.health.kafka.consumer.KafkaConsumerAgent
import com.metamx.health.kafka.consumer.RainerKafkaConsumer
import com.metamx.health.mesos.CuratorFrameworkBuilder
import com.metamx.health.mesos.HealthMesosConfig
import com.metamx.health.mesos.MesosAgent
import com.metamx.health.mesos.RainerMesos
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.health.ping.HealthPingConfig
import com.metamx.health.ping.PingAgent
import com.metamx.health.ping.RainerPing
import com.metamx.health.pipes.HealthPipesConfig
import com.metamx.health.pipes.PipesAgent
import com.metamx.health.pipes.RainerPipes
import com.metamx.health.rainer.RainerStack
import com.metamx.health.realtime.HealthRealtimeConfig
import com.metamx.health.realtime.RainerRealtime
import com.metamx.health.realtime.RealtimeAgent
import com.metamx.health.wagon.api.WagonHealthServlet
import com.metamx.metrics.JvmMonitor
import com.metamx.metrics.SysMonitor
import com.metamx.rainer.DbCommitStorageMySQLMixin
import java.util.Properties
import javax.servlet.Servlet
import javax.servlet.http.HttpServlet
import org.apache.curator.framework.CuratorFramework
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.skife.config.ConfigurationObjectFactory
import scala.collection.mutable.ArrayBuffer

object WagonMain extends AbstractMain with Logging
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

    val configDb = lifecycle(new MySQLDB(configs.apply[HealthDBConfig]) with DbCommitStorageMySQLMixin)
    val wagonConfig = configs.apply[WagonConfig]
    val agents = ArrayBuffer[PeriodicAgent]()
    val servlets = ArrayBuffer[(String, HttpServlet)]()

    // TODO: rethink pager notifications
    val pagerInventory: Option[NotifyInventory] = None

    if (wagonConfig.druidOn) {
      val druidConfig = configs.apply[HealthDruidConfig]
      val druidRainerStack = lifecycle(
        RainerStack.create[RainerDruid](
          curator,
          configDb,
          druidConfig,
          emitter
        )
      )
      agents += new DruidAgent(
        emitter,
        pagerInventory,
        discotheque,
        druidRainerStack.keeper,
        druidConfig
      )
      servlets += (("/config/druid/*", druidRainerStack.servlet))
    }

    if (wagonConfig.kafkaClusterOn) {
      val kafkaConfig = configs.apply[HealthKafkaClusterConfig]

      val kafkaClusterRainerStack = lifecycle(
        RainerStack.create[RainerKafkaCluster](
          curator,
          configDb,
          kafkaConfig,
          emitter
        )
      )
      agents += new KafkaClusterAgent(
        emitter,
        pagerInventory,
        kafkaClusterRainerStack.keeper,
        kafkaConfig
      )
      servlets += (("/config/kafka-cluster/*", kafkaClusterRainerStack.servlet))
    }

    if (wagonConfig.kafkaConsumerOn) {
      val kafkaConsumerConfig = configs.apply[HealthKafkaConsumerConfig]

      val kafkaConsumerRainerStack = lifecycle(
        RainerStack.create[RainerKafkaConsumer](
          curator,
          configDb,
          kafkaConsumerConfig,
          emitter
        )
      )
      agents += new KafkaConsumerAgent(
        emitter,
        pagerInventory,
        curator,
        kafkaConsumerRainerStack.keeper,
        kafkaConsumerConfig
      )
      servlets += (("/config/kafka-consumer/*", kafkaConsumerRainerStack.servlet))
    }

    if (wagonConfig.pingOn) {
      val pingConfig = configs.apply[HealthPingConfig]
      val pingRainerStack = lifecycle(
        RainerStack.create[RainerPing](
          curator,
          configDb,
          pingConfig,
          emitter
        )
      )
      val agent = new PingAgent(
        emitter,
        pagerInventory,
        discotheque,
        pingRainerStack.keeper,
        pingConfig,
        serviceConfig
      )
      agents += agent
      servlets += (("/config/http-ping/*", pingRainerStack.servlet))
      servlets += (("/list/http-ping/*", agent.listServlet))
    }

    if (wagonConfig.pipesOn) {
      val pipesConfig = configs.apply[HealthPipesConfig]
      val pipesRainerStack = lifecycle(
        RainerStack.create[RainerPipes](
          curator,
          configDb,
          pipesConfig,
          emitter
        )
      )
      agents += new PipesAgent(
        emitter,
        pagerInventory,
        discotheque,
        pipesRainerStack.keeper,
        pipesConfig
      )
      servlets += (("/config/pipes/*", pipesRainerStack.servlet))
    }

    if (wagonConfig.realtimeOn) {
      val realtimeConfig = configs.apply[HealthRealtimeConfig]
      val realtimeRainerStack = lifecycle(
        RainerStack.create[RainerRealtime](
          curator,
          configDb,
          realtimeConfig,
          emitter
        )
      )
      agents += new RealtimeAgent(
        lifecycle,
        emitter,
        pagerInventory,
        curator,
        discotheque,
        realtimeRainerStack.keeper,
        realtimeConfig
      )
      servlets += (("/config/realtime/*", realtimeRainerStack.servlet))
    }

    if (wagonConfig.mesosOn) {
      val curatorConfig = configs.apply[HealthCuratorConfig]
      val mesosConfig = configs.apply[HealthMesosConfig]
      val mesosRainer = lifecycle(
        RainerStack.create[RainerMesos](
          curator,
          configDb,
          mesosConfig,
          emitter
        )
      )

      agents += new MesosAgent(
        emitter,
        pagerInventory,
        mesosConfig,
        mesosRainer.keeper,
        new CuratorFrameworkBuilder(curatorConfig.zkConnect)
      )
      servlets += (("/config/mesos-cluster/*", mesosRainer.servlet))
    }

    if (wagonConfig.statefulCheckOn) {
      val statefulCheckConfig = configs.apply[HealthStatefulCheckConfig]
      val statefulCheckRainer = lifecycle(
        RainerStack.create[RainerStatefulCheck](
          curator,
          configDb,
          statefulCheckConfig,
          emitter
        )
      )
      val agent = new StatefulCheckAgent(
        emitter,
        pagerInventory,
        statefulCheckRainer.keeper,
        statefulCheckConfig,
        serviceConfig
      )
      agents += agent
      servlets += (("/config/stateful-check/*", statefulCheckRainer.servlet))
      servlets += (("/list/stateful-check/*", agent.listServlet))
    }

    servlets += (("/health", new WagonHealthServlet))

    initServletServer(
      serviceConfig.port,
      serviceConfig.maxThreads,
      lifecycle,
      servlets: _*
    )

    val periodicAgentsScheduler = new PeriodicAgentsScheduler(
      emitter,
      wagonConfig.pagerKey,
      pagerInventory,
      agents.toList
    )
    lifecycle(periodicAgentsScheduler)

    val agentsMonitors = agents.flatMap {
      case a: AlertAggregatorAgent => Some(a.alertAggregator.monitor)
      case _ => None
    }

    addMonitors(Seq(new SysMonitor, new JvmMonitor) ++ agentsMonitors)(lifecycle, configs, emitter)

    lifecycle.start()
    lifecycle.join()
  }

  def initServletServer(port: Int, maxThreads: Int, lifecycle: Lifecycle, servlets: (String, Servlet)*): Server = {
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
