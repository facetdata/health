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

package com.metamx.platform.common.kafka.inspect

import com.metamx.common.lifecycle.LifecycleStop
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.utils.ZKPaths
import scala.collection.JavaConverters._

class ConsumerOffsetMonitor(
  val group: String,
  val topic: String,
  offsetsCache: PathChildrenCache
)
{
  /**
   * Immutable view of the current state of consumer topic offsets.
   */
  def offsets: Map[Partition, Long] = {
    Consumers.zkOffsetsToMap(
      offsetsCache.getCurrentData.asScala.map { datum => (ZKPaths.getNodeFromPath(datum.getPath), datum.getData) }
    )
  }

  @LifecycleStop
  def stop(): Unit = {
    offsetsCache.close()
  }
}
