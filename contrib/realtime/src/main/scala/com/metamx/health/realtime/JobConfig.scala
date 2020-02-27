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

import com.metamx.common.scala.untyped._
import kafka.cluster.BrokerEndPoint
import org.apache.samza.system.{SystemStream => SamzaSystemStream}

class JobConfig(config: Dict)
{
  private val JobNameProperty = "job.name"
  private val TaskInputsProperty = "task.inputs"
  private val SystemsProducerBootstrapServersProperty = "systems.%s.producer.bootstrap.servers"
  private val SystemsProducerMetadataBrokerListProperty = "systems.%s.producer.metadata.broker.list"
  private val SystemsConsumerZookeeperConnectProperty =  "systems.%s.consumer.zookeeper.connect"
  private val SystemsProducerBootstrapServiceProperty = "systems.%s.producer.bootstrap.service"
  private val TaskCheckpointSystemProperty = "task.checkpoint.system"

  def jobName(): String = {
    str(config(JobNameProperty))
  }

  def taskCheckpointSystem(): String = {
    str(config(TaskCheckpointSystemProperty))
  }

  def checkpointBootstrapServers = bootstrapServers(taskCheckpointSystem())

  def checkpointZkPath = zkPath(taskCheckpointSystem())

  def checkpointBootstrapService = bootstrapService(taskCheckpointSystem())

  def taskInputs(): Seq[SystemStream] = {
    str(config(TaskInputsProperty)).split(",").map { x =>
      val (system, stream) = split2(x.trim, "\\.")
      SystemStream(system, stream)
    }.toSeq
  }

  def zkPath(system: String): String = {
    val zkPathProperty = SystemsConsumerZookeeperConnectProperty.format(system)
    config.get(zkPathProperty).map(str(_)).getOrElse {
      throw new IllegalStateException("%s wasn't found in config".format(zkPathProperty))
    }
  }

  def bootstrapService(system: String): String = {
    val service = SystemsProducerBootstrapServiceProperty.format(system)
    config.get(service).map(str(_)).getOrElse {
      throw new IllegalStateException("%s wasn't found in config".format(service))
    }
  }

  def bootstrapServers(system: String): Seq[BrokerEndPoint] = {
    val bootstrapServersProperty = SystemsProducerBootstrapServersProperty.format(system)
    val metadataBrokerListProperty = SystemsProducerMetadataBrokerListProperty.format(system)
    config.get(bootstrapServersProperty).map(x => parseBrokers(str(x))) getOrElse {
      config.get(metadataBrokerListProperty).map(x => parseBrokers(str(x))) getOrElse {
        throw new IllegalStateException("Neither %s nor %s was found in config".format(bootstrapServersProperty, metadataBrokerListProperty))
      }
    }
  }

  private def parseBrokers(str: String): Seq[BrokerEndPoint] = {
    str.split(",").map {
      x =>
        val (host, port) = split2(x.trim, ":")
        new BrokerEndPoint(-1, host, port.toInt)
    }.toSeq
  }

  private def split2(str: String, regex: String) = {
    str.split(regex) match {
      case Array(a, b) => (a, b)
      case _ => throw new IllegalStateException("Unable to split '%s' with '%s'".format(str, regex))
    }
  }
}

case class SystemStream(system:String, stream:String)

object SystemStream
{
  implicit def fromSamzaSystemStream(ss: SamzaSystemStream): SystemStream = SystemStream(ss.getSystem, ss.getStream)
}
