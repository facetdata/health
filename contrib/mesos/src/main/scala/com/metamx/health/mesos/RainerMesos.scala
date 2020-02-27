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

import com.metamx.common.scala.Yaml
import com.metamx.common.scala.untyped._
import com.metamx.rainer.{Commit, KeyValueDeserialization}
import java.io.ByteArrayInputStream
import java.net.URI

case class RainerMesos(
  zkMasterPath: String,
  zkEnsemble: Option[String],
  zkSessionTimeout: Int,
  zkConnectionTimeout: Int,
  zkMaxRetries: Int,
  zkBaseSleep: Int,
  exhibitorURI: Option[URI],
  exhibitorMaxRetries: Int,
  exhibitorBaseSleep: Int,
  exhibitorPollingMs: Int
)

object RainerMesos
{
  val DEFAULT_ZK_SESSION_TIMEOUT    = 60001
  val DEFAULT_ZK_CONNECTION_TIMEOUT = 1001
  val DEFAULT_ZK_MAX_RETRIES        = 30
  val DEFAULT_ZK_BASE_SLEEP         = 1002
  val DEFAULT_EXHIBITOR_MAX_RETRIES = 29
  val DEFAULT_EXHIBITOR_BASE_SLEEP  = 1003
  val DEFAULT_POLLING_MS            = 1004

  implicit val deserialization = new KeyValueDeserialization[RainerMesos] {
    override def fromKeyAndBytes(k: Commit.Key, bytes: Array[Byte]) = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))
      RainerMesos(
        str(d("zkMasterPath")),
        d.get("zkEnsemble").map(x => str(x)),
        int(d.getOrElse("zkSessionTimeout", DEFAULT_ZK_SESSION_TIMEOUT)),
        int(d.getOrElse("zkConnectionTimeout", DEFAULT_ZK_CONNECTION_TIMEOUT)),
        int(d.getOrElse("zkMaxRetries", DEFAULT_ZK_MAX_RETRIES)),
        int(d.getOrElse("zkBaseSleep", DEFAULT_ZK_BASE_SLEEP)),
        d.get("exhibitorURI").map(uri => new URI(str(uri))),
        int(d.getOrElse("exhibitorMaxRetries", DEFAULT_EXHIBITOR_MAX_RETRIES)),
        int(d.getOrElse("exhibitorBaseSleep", DEFAULT_EXHIBITOR_BASE_SLEEP)),
        int(d.getOrElse("exhibitorPollingMs", DEFAULT_POLLING_MS))
      )
    }
  }
}
