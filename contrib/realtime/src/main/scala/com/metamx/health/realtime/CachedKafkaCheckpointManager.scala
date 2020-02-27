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

import com.metamx.common.scala.Logging
import com.metamx.common.scala.collection.MapLikeOps
import com.metamx.common.scala.concurrent.loggingRunnable
import com.metamx.health.kafka.Kafka
import com.metamx.health.kafka.KafkaProperties
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import kafka.api.OffsetRequest
import kafka.api.PartitionMetadata
import kafka.cluster.BrokerEndPoint
import kafka.common.OffsetOutOfRangeException
import org.apache.kafka.common.utils.Utils
import org.apache.samza.checkpoint.kafka.KafkaCheckpointLogKey
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.serializers.CheckpointSerde
import org.joda.time.DateTime
import org.joda.time.Duration
import scala.collection.JavaConverters._
import scala.collection.mutable

class CachedKafkaCheckpointManager(config: CachedKafkaCheckpointManagerConfig, kafkaUtils: Kafka)
{
  val executorService = Executors
    .newScheduledThreadPool(config.threadPoolSize, new NamedPoolThreadFactory("CachedKafkaCheckpointManager-", true))
  val monitoredJobs = new ConcurrentHashMap[KafkaCheckpointMonitor, ScheduledFuture[_]]()

  def monitorJob(
    pipeline: String,
    jobName: String,
    bootstrapBrokers: Seq[BrokerEndPoint]
  ): KafkaCheckpointMonitor =
  {
    val monitoredJob = new KafkaCheckpointMonitorImpl(
      pipeline,
      jobName,
      bootstrapBrokers,
      kafkaUtils,
      config.kafkaProperties,
      config.timeout.toStandardDuration
    )
    val scheduled = executorService.scheduleAtFixedRate(
      monitoredJob.body,
      0L,
      config.checkpointsUpdatePeriod.toStandardDuration.getMillis,
      TimeUnit.MILLISECONDS
    )
    monitoredJobs.put(monitoredJob, scheduled)
    monitoredJob
  }

  def stopJobMonitoring(checkpointMonitor: KafkaCheckpointMonitor) = {
    val scheduled = monitoredJobs.remove(checkpointMonitor)
    if (scheduled != null) {
      scheduled.cancel(true)
    }
  }

  def close(): Unit ={
    executorService.shutdown()
  }
}

class KafkaCheckpointMonitorImpl(
  pipeline: String,
  jobName: String,
  bootstrapBrokers: Seq[BrokerEndPoint],
  kafkaUtils: Kafka,
  kafkaProps: KafkaProperties,
  timeout: Duration
) extends KafkaCheckpointMonitor with Logging
{
  KafkaCheckpointLogKey.setSystemStreamPartitionGrouperFactoryString(classOf[GroupByPartitionFactory].getCanonicalName)

  private val checkpointTopic = "__samza_checkpoint_ver_1_for_%s_1".format(jobName.replace("_", "-"))
  private val app = "%s-%s".format(pipeline, jobName)

  private val checkpointSerde = new CheckpointSerde

  private val startTime = DateTime.now()
  @volatile private var initialized: Boolean = false

  private var partitionsMetadata: Option[Seq[PartitionMetadata]] = None
  private var startingOffset: Option[Long] = None

  //Map(SystemStream -> Map(partition -> offset))
  private val _checkpoints = mutable.Map[SystemStream, mutable.Map[Int, Long]]()
  private var lastSuccessfulUpdateTime: Option[DateTime] = None
  private var failureException: Option[Throwable] = None

  private val checkpointsLock = new AnyRef

  val body = loggingRunnable {
    try {
      if (partitionsMetadata.isEmpty) {
        partitionsMetadata = kafkaUtils.fetchPartitionsMetadata(checkpointTopic, bootstrapBrokers, kafkaProps)
      }
      partitionsMetadata match {
        case Some(Seq(PartitionMetadata(partition, leaderOpt, _, _, _))) =>
          val leader = leaderOpt getOrElse {
            throw new IllegalStateException("No leader for checkpoints topic")
          }
          readCheckpoints(leader, partition)
          initialized = true

        case Some(psm) =>
          throw new IllegalStateException("Expected exactly one partition for checkpoint topic %s, got: %s".format(
            checkpointTopic,
            psm.map(_.partitionId).mkString("[", ", ", "]")
          ))

        case None =>
          // Checkpoint topic doesn't exist, but we can still calculate backlog
          initialized = true
      }

      checkpointsLock.synchronized {
        lastSuccessfulUpdateTime = Some(DateTime.now())
        failureException = None
      }
    } catch {
      case e: Exception =>
        partitionsMetadata = None
        checkpointsLock.synchronized {
          failureException = Some(e)
        }
        log.warn(e, "Exception while reading checkpoints for %s - topic %s".format(app, checkpointTopic))
    }
  }

  private def readCheckpoints(leader: BrokerEndPoint, partition: Int) {
    var offset = startingOffset getOrElse {
      kafkaUtils.fetchOffset(checkpointTopic, partition, kafkaProps, leader, OffsetRequest.EarliestTime)
    }
    val latestOffset = kafkaUtils.fetchOffset(
      checkpointTopic, partition, kafkaProps, leader, OffsetRequest.LatestTime
    )

    try {
      while (offset < latestOffset) {

        var empty = true
        for (response <- kafkaUtils.fetchMessages(checkpointTopic, partition, kafkaProps, leader, offset)) {
          if (!response.message.hasKey) {
            throw new IllegalStateException("Encountered message without key")
          }

          val key = KafkaCheckpointLogKey.fromBytes(Utils.readBytes(response.message.key))
          if (key.isCheckpointKey) {
            val bytes = Utils.readBytes(response.message.payload)

            // Sometimes CheckpointSerde is unable to deserialize checkpoint - in this case it will return null
            Option(checkpointSerde.fromBytes(bytes)) foreach {
              checkpoint =>
                val samzaCheckpointMap = checkpoint.getOffsets.asScala.toMap
                checkpointsLock.synchronized {
                  samzaCheckpointMap.foreach {
                    case (ssp, offsetString) =>
                      val currentOffsetMap = _checkpoints.getOrElseUpdate(ssp.getSystemStream, mutable.Map[Int, Long]())
                      currentOffsetMap(ssp.getPartition.getPartitionId) = offsetString.toLong
                  }
                }
            }
          }
          empty = false
          offset = response.nextOffset
          startingOffset = Some(offset)
        }
        if (empty) {
          throw new IllegalStateException("Fetched messages set is empty")
        }
      }
    } catch {
      case e: OffsetOutOfRangeException =>
        // Let's read from the beginning - checkpoint topics are pretty small
        startingOffset = None
        throw e
    }
  }

  override def checkpoints(): Try[Option[Map[SystemStream, Map[Int, Long]]]] = {
    checkpointsLock.synchronized {
      if (initialized) {
        lastSuccessfulUpdateTime.filter(_.plus(timeout).isBeforeNow).map {
          lastUpdate =>
            val message = "Checkpoints for %s - topic %s are outdated (last time updated at: %s)".format(app, checkpointTopic, lastUpdate)
            val exception = failureException match {
              case Some(e) => new Exception(message, e)
              case None => new Exception(message)
            }
            Throw(exception)
        }.getOrElse {
          Return(Some(_checkpoints.strictMapValues(_.toMap).toMap))
        }
      } else if (startTime.plus(timeout).isBeforeNow) {
        Throw(new Exception("CheckpointManager for %s - topic %s failed to initialize within %s".format(
          app, checkpointTopic, timeout
        )))
      } else {
        Return(None)
      }
    }
  }
}
