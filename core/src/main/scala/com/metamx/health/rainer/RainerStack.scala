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

package com.metamx.health.rainer

import com.metamx.common.lifecycle.{LifecycleStart, LifecycleStop}
import com.metamx.common.scala.db.DB
import com.metamx.common.scala.event.emit
import com.metamx.common.scala.{Logging, event}
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.core.config.HealthRainerConfig
import com.metamx.rainer.http.RainerServlet
import com.metamx.rainer.{CommitAutoPublisher, CommitKeeper, CommitStorage, DbCommitStorage, DbCommitStorageMixin, KeyValueDeserialization}
import com.twitter.util.Await
import org.apache.curator.framework.CuratorFramework

class RainerStack[ValueType : KeyValueDeserialization](
  val storage: CommitStorage[ValueType],
  val keeper: CommitKeeper[ValueType],
  autoPublisher: CommitAutoPublisher
)
{
  val servlet = new RainerServlet[ValueType] {
    override def valueDeserialization = implicitly[KeyValueDeserialization[ValueType]]

    override def commitStorage = storage
  }

  @LifecycleStart
  def start() {
    storage.start()
    autoPublisher.start()
  }

  @LifecycleStop
  def stop() {
    Await.result(autoPublisher.close())
    storage.stop()
  }
}

object RainerStack extends Logging
{
  def create[ValueType: KeyValueDeserialization](
    curator: CuratorFramework,
    db: DB with DbCommitStorageMixin,
    configConfig: HealthRainerConfig,
    emitter: ServiceEmitter
  ): RainerStack[ValueType] =
  {
    val keeper = new CommitKeeper[ValueType](curator, configConfig.zkPath)
    val storage = CommitStorage.keeperPublishing(
      new DbCommitStorage[ValueType](db, configConfig.table),
      keeper
    )
    val autoPublisher = keeper.autoPublisher(
      storage,
      configConfig.autoPublishDuration,
      configConfig.autoPublishFuzz,
      false,
      (e, commit) => emit.emitAlert(
        e, log, emitter, event.WARN, "Failed to publish commit: %s" format configConfig.table, Map(
          "table" -> configConfig.table,
          "commit" -> commit.meta.asMap
        )
      )
    )
    new RainerStack[ValueType](storage, keeper, autoPublisher)
  }
}
