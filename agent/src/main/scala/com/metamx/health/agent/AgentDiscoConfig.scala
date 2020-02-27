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

package com.metamx.health.agent

import com.metamx.common.scala.net.curator.CuratorConfig
import com.metamx.common.scala.net.curator.DiscoAnnounceConfig
import com.metamx.common.scala.net.curator.DiscoConfig
import org.joda.time.Duration

trait AgentDiscoConfig extends CuratorConfig with DiscoConfig
{
  val zkConnect: String

  val discoPath: String

  override def zkTimeout: Duration = Duration.parse("PT15S")

  override def discoAnnounce: Option[DiscoAnnounceConfig] = None
}
