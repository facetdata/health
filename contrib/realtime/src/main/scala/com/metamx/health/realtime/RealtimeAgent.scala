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

import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.collection.MapLikeOps
import com.metamx.common.scala.event._
import com.metamx.common.scala.net.curator._
import com.metamx.common.scala.option.OptionOps
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.AlertAggregatorAgent
import com.metamx.health.agent.HeartbeatingAgent
import com.metamx.health.agent.PeriodicAgent
import com.metamx.health.kafka._
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.http.client.HttpClientConfig
import com.metamx.http.client.HttpClientInit
import com.metamx.http.client.Request
import com.metamx.http.client.response.ToStringResponseHandler
import com.metamx.rainer.Commit
import com.metamx.rainer.CommitKeeper
import com.twitter.util._
import java.net.URI
import java.net.URL
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicReference
import org.apache.curator.framework.CuratorFramework
import org.jboss.netty.handler.codec.http.HttpMethod
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success

case class GuardianPipeline(env: String, guardian: String, pipeline: String)

class RealtimeAgent(
  lifecycle: Lifecycle,
  val emitter: ServiceEmitter,
  val pagerInventory: Option[NotifyInventory],
  curator: CuratorFramework,
  discotheque: Discotheque,
  keeper: CommitKeeper[RainerRealtime],
  config: HealthRealtimeConfig
) extends PeriodicAgent with HeartbeatingAgent with AlertAggregatorAgent with Logging
{
  override def period = config.period

  override def heartbeatPeriodMs = config.heartbeatPeriod.getMillis

  // Rainer key => Rainer config.
  private val guardians = new AtomicReference[Map[String, Commit[RainerRealtime]]]()

  // Rainer watcher shutter-downer.
  private lazy val closer = keeper.mirror().changes.register(Witness(guardians))

  private val httpClient = {
    val httpConfig = HttpClientConfig.builder()
      .withNumConnections(config.httpClientConnections)
      .withReadTimeout(config.httpClientTimeout.toStandardDuration)
      .build()
    HttpClientInit.createClient(httpConfig, lifecycle)
  }

  private val kafkaUtils = new Kafka(config.kafkaPoolSize, log)

  private val checkpointManager = new CachedKafkaCheckpointManager(config.toCheckpointManagerConfig, kafkaUtils)
  private val checkpointMonitors = new mutable.HashMap[GuardianPipeline, mutable.Map[String, KafkaCheckpointMonitor]]()
  private val checkpointMonitorsLock = new AnyRef

  private val hasPagerTriggered = mutable.Set[String]()

  private def failedToAnalyseOffsetIncidentKey(env: String, pipeline: String, job: String) =
    "kafka/realtime/analyse/%s/%s/%s".format(env, pipeline, job)

  private def pipelineBackloggedIncidentKey(system: String, consumerGroup: String, topic: String) =
    "kafka/realtime/backlogged/%s/%s/%s".format(system, consumerGroup, topic)

  private def guardianUnavailableIncidentKey(env: String) =
    "kafka/realtime/guardianUnavailable/%s".format(env)

  override def label: String = "realtime"

  override def init(): Unit = {
    // Force initialization.
    closer
  }

  override def probe(): Boolean = {
    dampedHeartbeat()

    val guardiansSnapshot = guardians.get().flatMap {
      case (env, commit) =>
        commit.value match {
          case None => None
          case Some(Right(x)) => Some(env -> x)
          case Some(Left(e)) =>
            val msg = "Bad config for realtime env[%s]".format(env)
            raiseAlert(e, Severity.ANOMALY, msg, Map.empty[String, Any])
            None
        }
    }

    val guardianPipelinesWithConfigs = for {
      (env, rainerConfig@RainerRealtime(_, _, guardianService, pagerKey, threshold)) <- guardiansSnapshot
      incidentKey = guardianUnavailableIncidentKey(env)
      guardianUri <- discotheque.disco(rainerConfig).instanceFor(guardianService).map(_.uri).ifEmpty {
        val message = "Failed to locate guardian service[%s] for env[%s]".format(guardianService, env)
        raiseAlert(Severity.ANOMALY, message, Map.empty)
        pagerKey.foreach { pk =>
          triggerPager(incidentKey, pk, message, Map.empty)
          hasPagerTriggered += incidentKey
        }
      }
    } yield {
      log.debug(
        "Got config for env[%s] guardian[%s] pagerKey[%s] and threshold[%s]"
          .format(env, guardianUri, pagerKey.getOrElse("None"), threshold)
      )

      for ( pk <- pagerKey if hasPagerTriggered.remove(incidentKey)) {
        resolvePager(incidentKey, pk)
      }

      fetchRunningJobs(guardianUri).map {
        case (pipeline, jobs) => GuardianPipeline(env, guardianService, pipeline) -> jobs
      }.toSeq
    }

    val jobsToMonitor = guardianPipelinesWithConfigs.flatten.toMap

    updateCheckpointManagers(jobsToMonitor)

    for ((gp, jobs) <- jobsToMonitor; job <- jobs) {
      val guardianParams = guardiansSnapshot(gp.env)
      val checkpointManager = checkpointMonitorsLock.synchronized {
        checkpointMonitors(gp)(job.jobName())
      }

      val deltas = analyse(gp, job, checkpointManager, config.toKafkaProperties, guardianParams.pagerKey)
      deltas.foreach {
        case (SystemStream(system, topic), deltasByPartition) =>
          emitMetricsAndAlerts(
            gp.env, guardianParams.threshold, guardianParams.pagerKey, deltasByPartition,
            system, topic, job.jobName(), job.zkPath(system)
          )
      }
    }

    true
  }

  private def emitMetricsAndAlerts(
    env: String,
    threshold: TopicThreshold,
    pagerKey: Option[String],
    deltasByPartition: Map[Int, PartitionDelta],
    system: String,
    topic: String,
    consumerGroup: String,
    kafkaZkPath: String
  ) = {
    var topicStat = TopicStat(threshold, deltasByPartition.size)

    deltasByPartition.foreach {
      case (partition, partitionDelta) =>
        val baseMetric = Metric(
          userDims = Map(
            "environment" -> Seq(env),
            "kafkaEnvironment" -> Seq(system),
            "kafkaConsumerGroup" -> Seq(consumerGroup),
            "kafkaTopic" -> Seq(topic),
            "kafkaZkPath" -> Seq(kafkaZkPath),
            "kafkaPartition" -> Seq(partition.toString)
          )
        )

        reportMetrics(Metric("consumer/offset/delta/messages", partitionDelta.messages) + baseMetric)
        reportMetrics(Metric("consumer/offset/delta/time", partitionDelta.time) + baseMetric)

        topicStat = topicStat.add(partition, partitionDelta)
    }

    log.info(
      "Kafka env[%s] system[%s] consumer[%s] topic[%s] has max partition delta: %s (threshold = %s)",
      env, system, consumerGroup, topic, topicStat.maxLagDelta, threshold
    )

    val message = "High latency for consumer: %s/%s, topic: %s" format(system, consumerGroup, topic)
    val details = Map(
      "env" -> env,
      "kafkaZkPath" -> kafkaZkPath,
      "consumerGroup" -> consumerGroup,
      "topic" -> topic
    )

    topicStat.alertMap.ifDefined { map => raiseAlert(ERROR, message, details ++ map) }

    val incidentKey = pipelineBackloggedIncidentKey(system, consumerGroup, topic)

    topicStat.pagerMap.ifDefined { pagerMap =>
      pagerKey.ifDefined { pagerKey =>
        triggerPager(incidentKey, pagerKey, message, details ++ pagerMap)
        hasPagerTriggered += incidentKey
      }.ifEmpty {
        raiseAlert(
          ERROR,
          "Pager threshold is reached but 'pagerKey' is empty for consumer: %s/%s, topic: %s"
            .format(system, consumerGroup, topic),
          details ++ pagerMap
        )
      }
    }.ifEmpty {
      for ( pk <- pagerKey if hasPagerTriggered.remove(incidentKey)) {
        resolvePager(incidentKey, pk)
      }
    }
  }

  private def updateCheckpointManagers(jobsToMonitor: Map[GuardianPipeline, Seq[JobConfig]]) = {
    checkpointMonitorsLock.synchronized {
      val jobsThatAreNotMonitoredAnyMore = checkpointMonitors.toMap.map {
        case (gp, checkpointManagersByJob) =>
          gp -> checkpointManagersByJob.filterKeys {
            monitoredJob =>
              jobsToMonitor.get(gp).flatMap {
                toMonitor =>
                  toMonitor.find {
                    configToMonitor =>
                      configToMonitor.jobName() == monitoredJob
                  }
              }.isEmpty
          }
      }
      jobsThatAreNotMonitoredAnyMore.foreach {
        case (gp, checkpointMonitorsByJob) =>
          checkpointMonitorsByJob.foreach {
            case (jobName, checkpointMonitor) =>
              checkpointMonitors(gp).remove(jobName)
              checkpointManager.stopJobMonitoring(checkpointMonitor)
          }
      }
      jobsToMonitor.foreach {
        case (gp, jobs) =>
          jobs.foreach {
            job =>
              checkpointMonitors.getOrElseUpdate(gp, mutable.Map[String, KafkaCheckpointMonitor]())
                .getOrElseUpdate(
                  job.jobName(),
                  checkpointManager.monitorJob(gp.pipeline, job.jobName(), job.checkpointBootstrapServers)
                )
          }
      }
    }
  }

  private def analyse(
    gp: GuardianPipeline,
    jobConfig: JobConfig,
    checkpointsMonitor: KafkaCheckpointMonitor,
    kafkaProperties: KafkaProperties,
    pagerKey: Option[String]
  ): Map[SystemStream, Map[Int, PartitionDelta]] = {
    val systemKafka = "kafka-"
    val jobName = jobConfig.jobName()
    checkpointsMonitor.checkpoints() match {
      case Return(Some(checkpoints)) =>
        val incidentKey = failedToAnalyseOffsetIncidentKey(gp.env, gp.pipeline, jobName)
        for (pk <- pagerKey if hasPagerTriggered.remove(incidentKey)) {
          resolvePager(incidentKey, pk)
        }

        val allPartitions: Map[String, Map[String, Seq[Int]]] = {
          val topicsBySystem = jobConfig.taskInputs().filter(_.system.startsWith(systemKafka)).groupBy(_.system).strictMapValues(_.map(_.stream))
          topicsBySystem.flatMap {
            case (system, topics) =>
              val partitions = try {
                val topicPartitions = kafkaUtils.fetchPartitionsMetadata(topics, jobConfig.bootstrapServers(system), kafkaProperties).flatMap {

                  case (topic, Some(meta)) => Some(topic -> meta.map(_.partitionId))

                  case (topic, None) =>
                    val message = "Unable to fetch metadata for topic %s in job %s".format(topic, jobName)
                    log.warn(message)
                    alertAggregator.put(
                      message, Map(
                        "jobName" -> jobName,
                        "system" -> system,
                        "topic" -> topic
                      )
                    )
                    None
                }
                Some(topicPartitions)
              } catch {
                case e: Throwable =>
                  val message = "Unable to fetch metadata for job %s from system %s".format(jobName, system)
                  log.warn(e, message)
                  alertAggregator.put(
                    e, message, Map(
                      "jobName" -> jobName,
                      "system" -> system,
                      "topics" -> topics
                    )
                  )
                  None
              }
              partitions.map(system -> _)
          }
        }

        val measuredDeltas = allPartitions.flatMap {
          case (system, topicPartitions) =>
            val topicPartitionOffsets = for (
              (topic, partitions) <- topicPartitions;
              partition <- partitions
            ) yield {
              val checkpoint = checkpoints.getOrElse(SystemStream(system, topic), Map.empty).getOrElse(partition, -1L)
              TopicPartitionOffset(topic, partition, checkpoint)
            }
            kafkaUtils.measureBacklog(topicPartitionOffsets.toSeq, jobConfig.bootstrapServers(system), kafkaProperties) match {
              case Success(deltas) =>
                for ((tpo, deltaWithAlerts) <- deltas) yield {
                  for (alert <- deltaWithAlerts.alerts) {
                    alertAggregator.put(
                      alert.description, Map(
                        "jobName" -> jobName,
                        "system" -> system,
                        "topic" -> tpo.topic,
                        "partition" -> tpo.partition
                      ) ++ alert.data
                    )
                  }
                  (SystemStream(system, tpo.topic), tpo.partition, deltaWithAlerts.delta)
                }

              case Failure(exception) =>
                alertAggregator.put(
                  exception,
                  "Unable to measure time delta for consumer: %s".format(jobName),
                  Map("system" -> system, "jobName" -> jobName)
                )
                None
            }
        }
        measuredDeltas.groupBy(item => item._1).strictMapValues(topicItems => topicItems.map( item => item._2 -> item._3).toMap)

      case Return(None) =>
        log.info("Checkpoint manager for job %s is not initialized yet".format(jobName))
        Map.empty

      case Throw(e) =>
        val details = Map(
          "system" -> jobConfig.taskCheckpointSystem(),
          "jobName" -> jobName,
          "exceptionType" -> e.getClass.getName,
          "exceptionMessage" -> e.getMessage,
          "exceptionStackTrace" -> e.getStackTraceString
        )
        val message = "Unable to read checkpoints for consumer: %s".format(jobName)
        pagerKey.ifDefined { pagerKey =>
          val incidentKey = failedToAnalyseOffsetIncidentKey(gp.env, gp.pipeline , jobName)
          triggerPager(incidentKey, pagerKey, message, details)
          hasPagerTriggered += incidentKey
        }
        alertAggregator.put(e, message, details)
        Map.empty
    }
  }

  private def fetchRunningJobs(guardian: URI): Map[String, Seq[JobConfig]] = {
    val pipelinesUrl = new URL(guardian.toURL, "/supervisor/pipelines")
    val listPipelinesRequest = new Request(HttpMethod.GET, pipelinesUrl)
    val responseFuture = httpClient.go(listPipelinesRequest, new ToStringResponseHandler(Charset.forName("UTF-8")))
    val pipelinesList = Jackson.parse[Dict](responseFuture.get()).get("results").map(list(_)).getOrElse(
      throw new IllegalStateException(
        "Key 'results' not found at guardian '%s'".format(pipelinesUrl)
      )
    )

    val activePipelines = pipelinesList.flatMap {
      pipelineMapAsAny =>
        val pipelineMap = dict(pipelineMapAsAny)
        val pipelineName = pipelineMap.get("pipeline").map(str(_)).getOrElse(
          throw new IllegalStateException("Key 'pipeline' not found in 'results' list at guardian '%s'".format(pipelinesUrl))
        )
        val pipelineJobs = pipelineMap.get("jobs").map(list(_)).getOrElse(
          throw new IllegalStateException("Key 'jobs' not found for pipeline '%s' at guardian '%s'".format(pipelineName, pipelinesUrl))
        )

        if (pipelineJobs.nonEmpty) Some(pipelineName) else None
    }

    activePipelines.map {
      pipelineName =>
        val explainUrl = new URL(guardian.toURL, "/supervisor/pipeline/%s/explain".format(pipelineName))
        val explainRequest = new Request(HttpMethod.GET, explainUrl)
        val explainResponseFuture = httpClient.go(explainRequest, new ToStringResponseHandler(Charset.forName("UTF-8")))
        val explainedJobsList =
          Jackson.parse[Dict](explainResponseFuture.get()).get("results").map(dict(_)).getOrElse(
            throw new IllegalStateException("Key 'results' not found in explanation at guardian '%s'".format(explainUrl))
          ).get("explanation").map(list(_)).getOrElse(
            throw new IllegalStateException("Key 'explanation' not found in 'results' object at guardian '%s'".format(explainUrl))
          )

        pipelineName -> explainedJobsList.map(job => new JobConfig(dict(job)))
    }.toMap
  }

  override def close(): Unit = {
    Await.result(closer.close())
    checkpointMonitorsLock.synchronized {
      checkpointMonitors.values.flatten.foreach { case (job, cm) => checkpointManager.stopJobMonitoring(cm) }
    }
    checkpointManager.close()
  }

}
