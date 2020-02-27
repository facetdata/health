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

package com.metamx.health.kafka.consumer

import com.metamx.common.scala.Logging
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.event._
import com.metamx.common.scala.option.OptionOps
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.AlertAggregatorAgent
import com.metamx.health.agent.HeartbeatingAgent
import com.metamx.health.agent.PeriodicAgent
import com.metamx.health.kafka._
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.platform.common.kafka.inspect.Brokers
import com.metamx.platform.common.kafka.inspect.ConsumerOffsetMonitor
import com.metamx.platform.common.kafka.inspect.Consumers
import com.metamx.platform.common.kafka.inspect.Partition
import com.metamx.rainer._
import com.twitter.util.Await
import com.twitter.util.Witness
import kafka.cluster.BrokerEndPoint
import org.apache.curator.framework.CuratorFramework
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success

class KafkaConsumerAgent(
  val emitter: ServiceEmitter,
  val pagerInventory: Option[NotifyInventory],
  curator: CuratorFramework,
  keeper: CommitKeeper[RainerKafkaConsumer],
  config: HealthKafkaConsumerConfig
) extends PeriodicAgent with HeartbeatingAgent with AlertAggregatorAgent with Logging
{

  class Things(
    val zkPath: String,
    val pagerKey: Option[String],
    val brokers: Brokers,
    val consumers: Consumers
  )
  {
    log.info("Tracking kafka environment located at: %s", zkPath)

    private val lock = new AnyRef

    // (consumerGroup, topic) -> (offset monitor, threshold)
    private val mutableOffsetMonitors = mutable.Map[(String, String), (ConsumerOffsetMonitor, TopicThreshold)]()

    // (consumerGroup, topic) -> threshold
    def adjust(pairs: Map[(String, String), TopicThreshold]) {
      // Close goner monitors. This is okay even though they may still be in use, because closing them just stops
      // updates and does not make them useless.
      lock.synchronized {
        mutableOffsetMonitors.filterKeys(!pairs.contains(_)) foreach {
          case ((consumerGroup, topic), (offsetMonitor, threshold)) =>
            log.info("Closing kafka consumer offset monitor for group: %s, topic: %s", consumerGroup, topic)
            mutableOffsetMonitors.remove((consumerGroup, topic))
            offsetMonitor.stop()
        }
      }
      // Add new monitors and adjust monitoring thresholds.
      for (((consumerGroup, topic), threshold) <- pairs) {
        lock.synchronized {
          if (!mutableOffsetMonitors.contains((consumerGroup, topic))) {
            log.info("Creating kafka consumer offset monitor for group: %s, topic: %s, threshold: %s",
              consumerGroup, topic, threshold)
            mutableOffsetMonitors.put(
              (consumerGroup, topic),
              (consumers.offsetMonitor(consumerGroup, topic), threshold)
            )
          } else if (mutableOffsetMonitors((consumerGroup, topic))._2 != threshold) {
            log.info(
              "Changing kafka consumer offset threshold for group: %s, topic: %s, to: %s",
              consumerGroup, topic, threshold)
            mutableOffsetMonitors.put(
              (consumerGroup, topic),
              (mutableOffsetMonitors((consumerGroup, topic))._1, threshold)
            )
          }
        }
      }
    }

    def offsetMonitors: Map[(String, String), (ConsumerOffsetMonitor, TopicThreshold)] = lock.synchronized {
      mutableOffsetMonitors.toMap
    }

    def close() {
      log.info("No longer tracking kafka environment located at: %s", zkPath)
      lock.synchronized {
        mutableOffsetMonitors.values foreach (_._1.stop())
      }
      brokers.stop()
    }
  }

  object Things
  {
    def fromConfig(rkc: RainerKafkaConsumer): Things = {
      val brokers = new Brokers(curator, rkc.zkPath)
      val consumers = new Consumers(curator, rkc.zkPath)
      brokers.start()
      new Things(rkc.zkPath, rkc.pagerKey, brokers, consumers)
    }
  }

  // env name -> Things
  private val kafkas = ConcurrentMap[String, Things]()

  private val kafkaUtils = new Kafka(config.kafkaPoolSize, log)

  private val isCurrentlyFailing = mutable.Map[String, Boolean]()

  private lazy val closer = {
    keeper.mirror().changes.register(new Witness[Map[Commit.Key, Commit[RainerKafkaConsumer]]] {
      override def notify(note: Map[Commit.Key, Commit[RainerKafkaConsumer]]) {
        val flatNote: Map[Commit.Key, RainerKafkaConsumer] = for {
          (env, commit) <- note
          rkc <- commit.valueOption
        } yield {
          (env, rkc)
        }
        // Remove goner keys.
        for ((env, things) <- kafkas) {
          if (!note.contains(env) || things.zkPath != flatNote(env).zkPath) {
            val goner = kafkas(env)
            kafkas.remove(env)
            goner.close()
          }
        }
        // Add new paths.
        for ((env, rkc) <- flatNote) {
          if (!kafkas.contains(env)) {
            kafkas.put(env, Things.fromConfig(rkc))
          }
          kafkas(env).adjust(rkc.thresholdAlert)
        }
      }
    })
  }

  @volatile private var started = true

  override def heartbeatPeriodMs = config.heartbeatPeriod.toStandardDuration.getMillis

  override def period = config.period

  override def label = "kafka consumer offsets"

  override def init() {
    // Force initialization.
    closer
  }

  override def probe() = {
    dampedHeartbeat()

    for (
      (env, things) <- kafkas;
      ((consumerGroup, topic), (offsetMonitor, threshold)) <- things.offsetMonitors
    ) {
      try {
        val consumerOffsets = offsetMonitor.offsets
        val brokerOffsets = things.brokers.getMaxOffsets(topic)

        val brokersList = things.brokers.brokerMap.values.map(b => BrokerEndPoint(b.id.toInt, b.host, b.port)).toSeq

        val tpos = for {
          (partition, brokerOffset) <- brokerOffsets
          consumerOffset <- consumerOffsets.get(partition)
        } yield {
          //offset saved with kafka high level consumer is offset of a next message after committed one
          val processedOffset = consumerOffset - 1
          TopicPartitionOffset(topic, partition.n, processedOffset)
        }
        //Result is map, where each record is (partition -> PartitionDelta(offsetDelta, timeDelta))
        val partitionDeltas  = kafkaUtils.measureBacklog(tpos.toSeq, brokersList, config.toKafkaProperties) match {
          case Success(deltas) =>
            for ((tpo, deltaWithAlerts) <- deltas) yield {
              for (alert <- deltaWithAlerts.alerts) {
                alertAggregator.put(
                  alert.description, Map(
                    "kafkaZkPath" -> things.zkPath,
                    "consumerGroup" -> consumerGroup,
                    "topic" -> topic,
                    "partition" -> tpo.partition
                  ) ++ alert.data
                )
              }
              (Partition(tpo.partition), deltaWithAlerts.delta)
            }

          case Failure(exception) =>
            alertAggregator.put(
              exception,
              "Unable to measure time delta for consumer: %s, topic: %s".format(consumerGroup, topic),
              Map(
                "kafkaZkPath" -> things.zkPath,
                "consumerGroup" -> consumerGroup,
                "topic" -> topic
              )
            )
            tpos.map {
              tpo =>
                val brokerOffset = brokerOffsets(Partition(tpo.partition))
                (Partition(tpo.partition), PartitionDelta(math.max(0L, brokerOffset - tpo.offset), 0L))
            }
        }

        // Only include partitions that exist on both brokers and consumers
        // Technically it would be better to include partitions that exist on the broker and *not* the consumer,
        // although this makes startup behavior strange (due to asynchronous population of caches)
        if (partitionDeltas.nonEmpty) {
          var topicStat = TopicStat(threshold, brokerOffsets.size)
          partitionDeltas.foreach {
            case (partition, partitionDelta) =>
              val baseMetric = Metric(
                userDims = Map(
                  "kafkaEnvironment" -> Seq(env),
                  "kafkaConsumerGroup" -> Seq(consumerGroup),
                  "kafkaTopic" -> Seq(topic),
                  "kafkaZkPath" -> Seq(things.zkPath),
                  "kafkaPartition" -> Seq(partition.n.toString)
                )
              )

              reportMetrics(Metric("consumer/offset/delta/messages", partitionDelta.messages) + baseMetric)
              reportMetrics(Metric("consumer/offset/delta/time", partitionDelta.time) + baseMetric)

              topicStat = topicStat.add(partition.n, partitionDelta)
          }
          log.info(
            "Kafka env[%s] consumer[%s] topic[%s] has max partition delta: %s (threshold = %s)",
            env, consumerGroup, topic, topicStat.maxLagDelta, threshold
          )

          val message = "High latency for consumer: %s/%s, topic: %s" format(env, consumerGroup, topic)
          val details = Map(
            "kafkaZkPath" -> things.zkPath,
            "consumerGroup" -> consumerGroup,
            "topic" -> topic
          )

          topicStat.alertMap.ifDefined { map => raiseAlert(ERROR, message, details ++ map) }

          val incidentKey = "kafka/consumer/%s/%s/%s".format(env, consumerGroup, topic)

          topicStat.pagerMap ifDefined { pagerMap =>
            isCurrentlyFailing(incidentKey) = true
            things.pagerKey.ifDefined { pagerKey =>
              triggerPager(incidentKey, pagerKey, message, details ++ pagerMap)
            } ifEmpty {
              raiseAlert(
                ERROR,
                "Pager threshold is reached but 'pagerKey' is empty for consumer: %s/%s, topic: %s"
                  .format(env, consumerGroup, topic),
                details ++ pagerMap
              )
            }
          } ifEmpty {
            things.pagerKey.ifDefined { pagerKey =>
              if (isCurrentlyFailing.getOrElse(incidentKey, false)) {
                resolvePager(incidentKey, pagerKey)
              }
            }
            isCurrentlyFailing(incidentKey) = false
          }

        } else {
          // Occurs on startup, likely due to asynchronous population of caches
          log.warn("No deltas found for env[%s] consumer[%s] topic[%s]", env, consumerGroup, topic)
        }
      } catch {
        case e: Throwable =>
          throw new RuntimeException("Unable to probe env[%s] zkPath[%s] topic[%s] consumerGroup[%s]".format(
            env, things.zkPath, topic, consumerGroup
          ), e)
      }
    }
    started
  }

  override def close() {
    started = false
    Await.result(closer.close())
  }
}
