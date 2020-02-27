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

package com.metamx.health.kafka.cluster

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.concurrent.locks.LockOps
import com.metamx.common.scala.option.OptionOps
import com.metamx.common.scala.untyped._
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import java.util.concurrent.locks.ReentrantLock
import kafka.common.Topic
import kafka.common.TopicAndPartition
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCacheListener

/**
  * Subscribes to zk for any changes of `topics` path of kafka cluster.
  * Keeps in memory assigned for partition brokers and brokers in isr.
  */
class TopicPartitionsState(curator: CuratorFramework, _basePath: String) extends Logging
{
  private val basePath = _basePath.stripPrefix("/").stripSuffix("/") match {
    case "" => ""
    case x => "/" + x
  }

  private val topicsPath = "%s/brokers/topics".format(basePath)
  private val topicRegex = "%s/(%s+)".format(topicsPath, Topic.legalChars).r
  private val stateRegex = "%s/(%s+)/partitions/([0-9]+)/state".format(topicsPath, Topic.legalChars).r

  private val topicsCache = new TreeCache(curator, topicsPath)

  topicsCache.getListenable.addListener(
    new TreeCacheListener {
      override def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = {
        event.getType match {
          case TreeCacheEvent.Type.NODE_ADDED | TreeCacheEvent.Type.NODE_UPDATED =>
            event.getData.getPath match {
              case topicRegex(topic) =>
                val topicData = Jackson.parse[Dict](event.getData.getData)
                for ((partition, assignedList) <- dict(topicData("partitions"))) {
                  setAssigned(TopicAndPartition(topic, partition.toInt), list(assignedList).map(int(_)))
                }

              case stateRegex(topic, partition) =>
                val stateData = Jackson.parse[Dict](event.getData.getData)
                setIsr(TopicAndPartition(topic, partition.toInt), list(stateData("isr")).map(int(_)))

              case _ =>
            }

          case TreeCacheEvent.Type.NODE_REMOVED =>
            event.getData.getPath match {
              case topicRegex(topic) => deleteTopic(topic)
              case stateRegex(topic, partition) => deleteTopicAndPartition(TopicAndPartition(topic, partition.toInt))
              case _ =>
            }

          case TreeCacheEvent.Type.INITIALIZED =>
            lock {
              log.info("Partition watcher initialized for zkPath[%s]".format(topicsPath))
              initialized = true
              initializedCondition.signalAll()
            }

          case _ =>
          //Ignore
        }
      }
    }
  )

  private           val lock                 = new ReentrantLock()
  private           val initializedCondition = lock.newCondition()
  @volatile private var initialized          = false

  private val topicsAndPartitions       = ConcurrentMap[TopicAndPartition, PartitionState]()
  private val offlinePartitions         = ConcurrentMap[TopicAndPartition, DateTime]()
  private val underReplicatedPartitions = ConcurrentMap[TopicAndPartition, DateTime]()

  private def updateUnderReplicated(topicAndPartition: TopicAndPartition, state: PartitionState): Unit = {
    if (state.isr.isEmpty) {
      val time = offlinePartitions.getOrElseUpdate(topicAndPartition, DateTime.now)
      log.debug("%s is offline since %s".format(topicAndPartition, time))
    } else {
      removeFromOffline(topicAndPartition)
    }

    if (state.assigned.diff(state.isr).nonEmpty) {
      val time = underReplicatedPartitions.getOrElseUpdate(topicAndPartition, DateTime.now)
      log.debug("%s is under-replicated since %s".format(topicAndPartition, time))
    } else {
      removeFromUnderReplicated(topicAndPartition)
    }
  }

  private def removeFromOffline(topicAndPartition: TopicAndPartition) = {
    val removed = offlinePartitions.remove(topicAndPartition)
    removed.ifDefined { time =>
      log.debug("Remove %s from offline partitions which was offline since %s, ".format(
        topicAndPartition, time
      ))
    }
  }

  private def removeFromUnderReplicated(topicAndPartition: TopicAndPartition) = {
    val removed = underReplicatedPartitions.remove(topicAndPartition)
    removed.ifDefined { time =>
      log.debug("Remove %s from under-replicated partitions which was under-replicated since %s, ".format(
        topicAndPartition, time
      ))
    }
  }

  private def setAssigned(topicAndPartition: TopicAndPartition, assigned: Seq[Int]): Unit = {
    val state = topicsAndPartitions.getOrElse(topicAndPartition, new PartitionState(assigned, Seq[Int]()))
    val newState = new PartitionState(assigned, state.isr)
    topicsAndPartitions(topicAndPartition) = newState
    updateUnderReplicated(topicAndPartition, newState)
  }

  private def setIsr(topicAndPartition: TopicAndPartition, isr: Seq[Int]): Unit = {
    val state = topicsAndPartitions.getOrElse(topicAndPartition, new PartitionState(Seq[Int](), isr))
    val newState = new PartitionState(state.assigned, isr)
    topicsAndPartitions(topicAndPartition) = newState
    updateUnderReplicated(topicAndPartition, newState)
  }

  private def deleteTopic(topic: String): Unit = {
    topicsAndPartitions.foreach { case (topicAndPartition, _) =>
      if (topicAndPartition.topic == topic) {
        deleteTopicAndPartition(topicAndPartition)
      }
    }
  }

  private def deleteTopicAndPartition(topicAndPartition: TopicAndPartition): Unit = {
    topicsAndPartitions.remove(topicAndPartition)
    removeFromOffline(topicAndPartition)
    removeFromUnderReplicated(topicAndPartition)
  }

  private def whenInitialized[X](f: => X): X = {
    lock {
      while (!initialized) {
        initializedCondition.await()
      }
      f
    }
  }

  /**
    * Gets partitions in cluster.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def partitions(): Set[TopicAndPartition] = whenInitialized(topicsAndPartitions.keySet.toSet)

  /**
    * Gets partitions count in cluster.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def partitionsCount(): Int = whenInitialized(topicsAndPartitions.size)

  /**
    * Gets offline partitions in cluster.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def offline(): Map[TopicAndPartition, DateTime] = whenInitialized(offlinePartitions.toMap)

  /**
    * Gets offline partitions count in cluster.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def offlineCount(): Int = whenInitialized(offlinePartitions.size)

  /**
    * Gets partitions what are offline at least from `dateTime`.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def offlineSince(dateTime: DateTime): Map[TopicAndPartition, DateTime] = {
    whenInitialized(offlinePartitions.filter { case (_, since) => since < dateTime }.toMap)
  }

  /**
    * Gets count of partitions what are offline at least from `dateTime`.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def offlineSinceCount(dateTime: DateTime): Int =
  whenInitialized(offlinePartitions.count { case (_, since) => since < dateTime })

  /**
    * Gets under-replicated partitions in cluster.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def underReplicated(): Map[TopicAndPartition, DateTime] = whenInitialized(underReplicatedPartitions.toMap)

  /**
    * Gets under-replicated partitions count in cluster.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def underReplicatedCount(): Int = whenInitialized(underReplicatedPartitions.size)

  /**
    * Gets partitions what are under-replicated at least from `dateTime`.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def underReplicatedSince(dateTime: DateTime): Map[TopicAndPartition, DateTime] = {
    whenInitialized(underReplicatedPartitions.filter { case (_, since) => since < dateTime }.toMap)
  }

  /**
    * Gets count of partitions what are under-replicated at least from `dateTime`.
    * Thread can be blocked on this method until PathCache will be initialized.
    */
  def underReplicatedSinceCount(dateTime: DateTime): Int =
    whenInitialized(underReplicatedPartitions.count { case (_, since) => since < dateTime })

  def start(): Unit = {
    topicsCache.start()
  }

  def stop(): Unit = {
    topicsCache.close()
  }
}

class PartitionState(
  val assigned: Seq[Int],
  val isr: Seq[Int]
)
