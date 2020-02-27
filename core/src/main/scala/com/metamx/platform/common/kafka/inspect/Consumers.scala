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

import com.google.common.base.Charsets
import com.metamx.common.scala.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.KeeperException.NoNodeException
import scala.collection.JavaConverters._

/**
  * Inspects the state of high-level kafka consumers.
  */
class Consumers(curator: CuratorFramework, _basePath: String) extends Logging
{
  private[this] val basePath = _basePath.stripPrefix("/").stripSuffix("/") match {
    case "" => ""
    case x => "/" + x
  }

  private[this] def groupsPath = {
    "%s/consumers".format(basePath)
  }

  private[this] def topicsPath(group: String) = {
    "%s/consumers/%s/offsets".format(basePath, group)
  }

  private[this] def topicOffsetsPath(group: String, topic: String) = {
    "%s/consumers/%s/offsets/%s".format(basePath, group, topic)
  }

  /**
    * Fetches current consumer groups. Not cached; each call to this method results in a ZK fetch.
    */
  def getGroups(): Seq[String] = {
    try {
      curator.getChildren.forPath(groupsPath).asScala
    } catch {
      case _: NoNodeException => Nil
    }
  }

  /**
    * Fetches current committed offsets for a particular consumer group and topic. Not cached; each call to this
    * method results in a ZK fetch.
    */
  def getTopics(group: String): Seq[String] = {
    try {
      curator.getChildren.forPath(topicsPath(group)).asScala
    } catch {
      case _: NoNodeException => Nil
    }
  }

  /**
    * Fetches current committed offsets for a particular consumer group and topic. Not cached; each call to this
    * method results in a flurry of ZK fetches.
    */
  def getOffsets(group: String, topic: String): Map[Partition, Long] = {
    val children = try {
      curator.getChildren.forPath(topicOffsetsPath(group, topic)).asScala
    } catch {
      case _: NoNodeException => Nil
    }
    Consumers.zkOffsetsToMap(
      children map
        (c => (c, curator.getData.forPath(ZKPaths.makePath(topicOffsetsPath(group, topic), c))))
    )
  }

  def offsetMonitor(group: String, topic: String): ConsumerOffsetMonitor = {
    val offsetsCache = new PathChildrenCache(curator, topicOffsetsPath(group, topic), true)
    offsetsCache.start()
    new ConsumerOffsetMonitor(group, topic, offsetsCache)
  }
}

object Consumers
{
  def zkOffsetsToMap(children: Seq[(String, Array[Byte])]): Map[Partition, Long] = {
    children.map {
      case (partitionNum, offsetData) => (Partition(partitionNum.toInt), new String(offsetData, Charsets.UTF_8).toLong)
    }.toMap
  }
}
