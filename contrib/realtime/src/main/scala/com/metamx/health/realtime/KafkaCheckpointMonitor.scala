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

package com.metamx.health.realtime

import com.twitter.util.Try

trait KafkaCheckpointMonitor
{
  /**
    *
    * @return
    *       Return(Some(checkpointMap)) - where checkpoint map is Map( systemStream -> Map(partition -> offset))
    *       Return(None) - if CheckpointManager is not initialized yet
    *       Throw(e) - if CheckpointManager failed to read checkpoints
    */
  def checkpoints(): Try[Option[Map[SystemStream, Map[Int, Long]]]]
}
