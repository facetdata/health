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

import com.github.nscala_time.time.Imports._
import com.metamx.common.Props
import com.metamx.common.concurrent.ScheduledExecutors
import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.Abort
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.config._
import com.metamx.common.scala.net.curator.Curator
import com.metamx.common.scala.net.curator.Discotheque
import com.metamx.emitter.core.Emitters
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.core.config.HealthCuratorConfig
import com.metamx.health.core.config.ServiceConfig
import com.metamx.http.client.HttpClientConfig
import com.metamx.http.client.HttpClientInit
import com.metamx.metrics._
import java.util.Properties
import javax.net.ssl.SSLContext
import org.apache.curator.framework.CuratorFramework
import org.skife.config.ConfigurationObjectFactory
import scala.collection.JavaConverters._

abstract class AbstractMain
{
  protected def main(
    lifecycle: Lifecycle,
    configs: ConfigurationObjectFactory,
    props: Properties,
    emitter: ServiceEmitter,
    curator: CuratorFramework,
    discotheque: Discotheque
  )

  def main(args: Array[String]) {
    val props = args match {
      case Array(filename) =>
        new Properties withEffect {
          p => p.putAll(Props.fromFilename(filename))
        }

      case _ =>
        System.err.println("Usage: $0 <runtime.properties>")
        System.exit(1)
        throw new AssertionError("#notreached")
    }

    try {
      val lifecycle = new Lifecycle
      val configs = new ConfigurationObjectFactory(props)

      // Emitter
      val serviceConfig = configs.apply[ServiceConfig]
      val mmxHttpClient = HttpClientInit.createClient(
        new HttpClientConfig(1, SSLContext.getDefault, serviceConfig.emitterTimeout.standardDuration),
        lifecycle
      )
      val emitter = new ServiceEmitter(
        serviceConfig.service,
        "%s:%s".format(serviceConfig.host, serviceConfig.port),
        Emitters.create(props, mmxHttpClient, lifecycle)
      )

      // Curator, service discovery
      val curatorConfig = configs.apply[HealthCuratorConfig]
      val curator = Curator.create(curatorConfig, lifecycle)
      val discotheque = new Discotheque(lifecycle)

      discotheque.disco(configs.apply[HealthCuratorConfig]) // Need this to create default disco and announce service

      main(lifecycle, configs, props, emitter, curator, discotheque)
    }
    catch {
      case e: Throwable => Abort(e)
    }
  }

  def addMonitors(monitors: Seq[Monitor])(
    lifecycle: Lifecycle,
    configs: ConfigurationObjectFactory,
    emitter: ServiceEmitter
  ) {
    lifecycle.addManagedInstance(
      new MonitorScheduler(
        configs.apply[MonitorSchedulerConfig],
        ScheduledExecutors.createFactory(lifecycle).create(1, "MonitorScheduler"),
        emitter,
        monitors.asJava
      )
    )
  }
}
