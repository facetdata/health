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

import com.metamx.common.lifecycle.LifecycleStart
import com.metamx.common.lifecycle.LifecycleStop
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef.FinallyOps
import com.metamx.common.scala.collection.MapLikeOps
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.untyped.list
import com.metamx.platform.common.kafka.inspect.Brokers._
import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.api.PartitionOffsetsResponse
import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
import org.apache.curator.utils.ZKPaths
import scala.collection.JavaConverters._
import scala.collection.mutable

case class Partition(n: Int)
{
  override def toString: String = "%s".format(n)
}

class PartitionInfo(val leader: Option[BrokerEndPoint], val isr: Seq[BrokerEndPoint], val partition: Partition)
{
  override def toString: String = "[%s] Leader:%s Isr:%s".format(
    partition,
    leader.map(_.id).getOrElse("-1"),
    isr.map(_.id).sortBy(id => id).mkString("", ",", "")
  )
}

object Brokers
{
  val BrokerStateHost = "host"
  val BrokerStatePort = "port"

  val PartitionStateLeader = "leader"
  val PartitionStateISR = "isr"
}

/**
  * Uses Curator caches to watch kafka brokers and keep an up-to-date view on what topics are available where. May
  * not have all available information immediately upon start, due to the nature of the caches.
  */
class Brokers(curator: CuratorFramework, _basePath: String) extends Logging
{
  private[this] val basePath = _basePath.stripPrefix("/").stripSuffix("/") match {
    case "" => ""
    case x => "/" + x
  }

  private[this] def brokersPath = "%s/brokers/ids".format(basePath)

  private[this] def topicsPath = "%s/brokers/topics".format(basePath)

  private[this] def topicPath(topic: String) = "%s/%s/partitions".format(topicsPath, topic)

  private[this] def partitionStatePath(partition: String) = "%s/state".format(partition)

  @volatile private[this] var started = false

  // List of brokers
  private[this] val brokerCache = new PathChildrenCache(curator, brokersPath, true)

  // List of topics
  private[this] val topicsCache = new PathChildrenCache(curator, topicsPath, false)

  // Topic -> broker/partition map. Access must be synchronized.
  private[this] val topicCaches = mutable.HashMap[String, PathChildrenCache]()

  // Initial cache population is complete
  private[this] var initialized = 1

  private[this] val initializedMonitor = new AnyRef

  // Make sure we have something in topicCaches for each topic
  topicsCache.getListenable.addListener(
    new PathChildrenCacheListener
    {
      def childEvent(_curator: CuratorFramework, event: PathChildrenCacheEvent) {
        require(curator eq _curator, "WTF?! Got an unexpected curator instance!")
        def topic = ZKPaths.getNodeFromPath(event.getData.getPath)

        if (started) {
          topicCaches.synchronized {
            event.getType match {
              case PathChildrenCacheEvent.Type.CHILD_ADDED | PathChildrenCacheEvent.Type.CHILD_UPDATED =>
                if (!topicCaches.contains(topic)) {
                  topicCaches(topic) = new PathChildrenCache(curator, topicPath(topic), true)
                  topicCaches(topic).getListenable.addListener(
                    new PathChildrenCacheListener {
                      def childEvent(_client: CuratorFramework, _event: PathChildrenCacheEvent) {
                        if (_event.getType == PathChildrenCacheEvent.Type.INITIALIZED) {
                          initializedMonitor.synchronized {
                            initialized -= 1
                            initializedMonitor.notifyAll()
                          }
                        }
                      }
                    }
                  )
                  topicCaches(topic).start(StartMode.POST_INITIALIZED_EVENT)
                }

              case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
                if (topicCaches.contains(topic)) {
                  try topicCaches(topic).close()
                  catch {
                    case e: Exception =>
                      log.warn("Unable to close topic cache: %s", topic)
                  }
                  topicCaches.remove(topic)
                }

              case PathChildrenCacheEvent.Type.INITIALIZED =>
                initializedMonitor.synchronized {
                  initialized += topicCaches.size
                  initialized -= 1
                  initializedMonitor.notifyAll()
                }

              case PathChildrenCacheEvent.Type.INITIALIZED |
                   PathChildrenCacheEvent.Type.CONNECTION_LOST |
                   PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED |
                   PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED =>
              // Ignore
            }
          }
        }
      }
    }
  )

  /**
    * Immutable view of current brokers. Represented as a map indexed by broker id.
    */
  def brokerMap: Map[Int, BrokerEndPoint] = {
    val data = brokerCache.getCurrentData.asScala
    data.map {
      datum =>
        val id = ZKPaths.getNodeFromPath(datum.getPath).toInt
        val dict = Jackson.parse[Dict](datum.getData)
        val host = dict(BrokerStateHost).toString
        val port = dict(BrokerStatePort).toString
        id -> BrokerEndPoint(id, host, port.toInt)
    }.toMap
  }

  /**
    * Immutable view of the current state of a topic.
    * Not cached; each call to this method results in a flurry of ZK fetches.
    */
  def topicPartitions(topic: String): Seq[PartitionInfo] = {
    topicCaches.synchronized {
      topicCaches.get(topic).toSeq
    }.flatMap {
      topicCache =>
        val topicDatums = topicCache.getCurrentData.asScala
        topicDatums.map {
          topicDatum =>

            val partition = ZKPaths.getNodeFromPath(topicDatum.getPath)
            val n = partition.toInt

            val stateArr = curator.getData.forPath(partitionStatePath(topicDatum.getPath))
            val state = Jackson.parse[Dict](stateArr)

            val leaderId = state.get(PartitionStateLeader).map(_.toString.toInt)
            val isrIds = state.get(PartitionStateISR).map(isr => list(isr).map(_.toString.toInt)).getOrElse(Seq())

            val leader = leaderId.flatMap(brokerMap.get)
            val isr = isrIds.flatMap(brokerMap.get)

            new PartitionInfo(leader, isr, Partition(n))
        }
    }
  }

  def topics(): Set[String] = {
    topicCaches.synchronized {
      topicCaches.keySet.toSet
    }
  }

  /**
    * Shorthand for getOffsetsBefore(topic, -1).
    */
  def getMaxOffsets(topic: String): Map[Partition, Long] = getOffsetsBefore(topic, -1)

  /**
    * Uses SimpleConsumers to call getOffsetsBefore on every partition of a particular topic. The consumers are shut
    * down immediately after the call is finished.
    */
  def getOffsetsBefore(topic: String, time: Long): Map[Partition, Long] = {
    topicPartitions(topic).groupBy(x => x.leader).flatMap {
      case (leaderOpt, partitions) =>
        // Requerying leader
        leaderOpt.flatMap(leader => brokerMap.get(leader.id)).map(leader => (leader, partitions))
    }.flatMap {
      case (BrokerEndPoint(_, host, port), brokerPartitions) =>
        new SimpleConsumer(host, port, 30000, 10 * 1024 * 1024, "get-offsets-before").withFinally(_.close()) {
          consumer =>

            val offsetResponse = consumer.getOffsetsBefore(
              OffsetRequest(
                brokerPartitions.map((partitionInfo: PartitionInfo) =>
                  TopicAndPartition(topic, partitionInfo.partition.n) -> PartitionOffsetRequestInfo(time, 1)
                ).toMap
              )
            )

            offsetResponse.partitionErrorAndOffsets
              .strictFilterKeys(_.topic == topic)
              .flatMap {
                case (TopicAndPartition(t, partition), PartitionOffsetsResponse(_, offsets)) =>
                  offsets.headOption.map(
                    offset => Partition(partition) -> offset
                  )
              }
        }
    }
  }

  /**
    * Wait for initialization.
    */
  def await(): Unit = {
    initializedMonitor.synchronized {
      while (initialized > 0) {
        initializedMonitor.wait()
      }
    }
  }

  @LifecycleStart
  def start(): Unit = {
    brokerCache.start(StartMode.BUILD_INITIAL_CACHE)
    topicsCache.start(StartMode.POST_INITIALIZED_EVENT)
    started = true
  }

  @LifecycleStop
  def stop(): Unit = {
    started = false
    brokerCache.close()
    topicsCache.close()
    topicCaches.synchronized {
      topicCaches.values.foreach(_.close())
    }
  }
}
