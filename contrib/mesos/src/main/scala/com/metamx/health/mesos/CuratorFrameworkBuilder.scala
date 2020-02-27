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

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableList
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import org.apache.curator.ensemble.exhibitor.Exhibitors.BackupConnectionStringProvider
import org.apache.curator.ensemble.exhibitor.{DefaultExhibitorRestClient, ExhibitorEnsembleProvider, ExhibitorRestClient, Exhibitors}
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

class CuratorFrameworkBuilder(
  defaultEnsemble: String,
  @VisibleForTesting builder: CuratorFrameworkFactory.Builder = CuratorFrameworkFactory.builder,
  @VisibleForTesting client: ExhibitorRestClient = new DefaultExhibitorRestClient()
)
{
  def apply(rainerMesos: RainerMesos): CuratorFramework = {
    val provider = rainerMesos.exhibitorURI match {
      case Some(uri) =>
        new ExhibitorEnsembleProvider(
          new Exhibitors(
            ImmutableList.of(uri.getHost),
            uri.getPort,
            new BackupConnectionStringProvider()
            {
              override def getBackupConnectionString: String = rainerMesos.zkEnsemble.getOrElse(defaultEnsemble)
            }
          ),
          client,
          uri.getPath,
          rainerMesos.exhibitorPollingMs,
          new ExponentialBackoffRetry(rainerMesos.exhibitorBaseSleep, rainerMesos.exhibitorMaxRetries)
        )

      case _ =>
        new FixedEnsembleProvider(rainerMesos.zkEnsemble.getOrElse(defaultEnsemble))
    }

    val curatorFramework = builder
      .connectionTimeoutMs(rainerMesos.zkConnectionTimeout)
      .sessionTimeoutMs(rainerMesos.zkSessionTimeout)
      .retryPolicy(new ExponentialBackoffRetry(rainerMesos.zkBaseSleep, rainerMesos.zkMaxRetries))
      .ensembleProvider(provider)
      .build()

    builder.getEnsembleProvider match {
      case provider: ExhibitorEnsembleProvider => createProxy(provider, curatorFramework)
      case _ => curatorFramework
    }
  }

  /**
    * We have to synchronously poll an exhibitor on the curator framework start, otherwise we could get a broken
    * Zookeeper instance due to race condition.
    */
  @VisibleForTesting
  private [mesos] def createProxy(provider: ExhibitorEnsembleProvider, curatorFramework: CuratorFramework) = {
    Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array(classOf[CuratorFramework]),
      new InvocationHandler
      {
        override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = {
          if (method.getName == "start") {
            provider.pollForInitialEnsemble()
          }

          method.invoke(curatorFramework, args: _*)
        }
      }
    ).asInstanceOf[CuratorFramework]
  }
}
