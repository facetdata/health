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
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef.EffectOps
import com.metamx.common.scala.collection.implicits.TraversableOnceOps
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.concurrent.locks.LockOps
import com.metamx.common.scala.option.OptionOps
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.agent.HeartbeatingAgent
import com.metamx.health.agent.PeriodicAgent
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.health.util.Jmx
import com.metamx.health.util.PeriodUtil
import com.metamx.platform.common.kafka.inspect.Brokers
import com.metamx.rainer.Commit
import com.metamx.rainer.Commit.Key
import com.metamx.rainer.CommitKeeper
import com.twitter.util._
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kafka.cluster.BrokerEndPoint
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.joda.time.Period

class KafkaClusterAgent(
  val emitter: ServiceEmitter,
  val pagerInventory: Option[NotifyInventory],
  keeper: CommitKeeper[RainerKafkaCluster],
  config: HealthKafkaClusterConfig
) extends PeriodicAgent with HeartbeatingAgent with Logging
{

  /**
    * Objects of this class are immutable. All operations create copies
    */
  class Things(
    val config: RainerKafkaCluster,
    val brokers: Brokers,
    val topicPartitionsState: TopicPartitionsState,
    curator: CuratorFramework
  )
  {
    def apply(config: RainerKafkaCluster): Things = {
      new Things(config, this.brokers, this.topicPartitionsState, this.curator)
    }

    def close() {
      log.info("No longer tracking kafka env located at: %s", config.zkConnect)

      brokers.stop()
      topicPartitionsState.stop()
      curator.close()
    }
  }

  object Things
  {
    def apply(rk: RainerKafkaCluster): Things = {
      val curator = buildCurator(rk)
      //kafka path supposed to be set via zkConnect chroot
      val brokers = new Brokers(curator, "/")
      brokers.start()

      val underReplicatedState = new TopicPartitionsState(curator, "/")
      underReplicatedState.start()

      new Things(rk, brokers, underReplicatedState, curator)
    }

    private def buildCurator(curatorConfig: RainerKafkaCluster) = {
      CuratorFrameworkFactory
        .builder()
        .connectString(curatorConfig.zkConnect)
        .sessionTimeoutMs(curatorConfig.zkTimeout.getMillis.toInt)
        .retryPolicy(new ExponentialBackoffRetry(1000, 30))
        .build()
        .withEffect(_.start)
    }
  }

  case class BrokerValue(brokerId: Option[Int], value: Double)

  type Environment = String
  type Service = String

  override def period: Period = config.period

  override def heartbeatPeriodMs = config.heartbeatPeriod.toStandardDuration.getMillis

  private val defaultJmxPort = 17071

  private val kafkas = ConcurrentMap[Environment, Things]()

  private val isCurrentlyFailing = ConcurrentMap[Service, Boolean]()
  private val previousUncleanValues = ConcurrentMap[Service, BrokerValue]()

  private val kafkasLock  = new ReentrantLock()
  private val uncleanLock = new ReentrantLock()

  private val pool = FuturePool(Executors.newFixedThreadPool(
    config.threads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("KafkaClusterAgent-%d").build()
  ))

  private lazy val closer = {
    keeper.mirror().changes.register(new Witness[Map[Key, Commit[RainerKafkaCluster]]] {
      override def notify(note: Map[Key, Commit[RainerKafkaCluster]]): Unit = {
        val flatNote: Map[Commit.Key, RainerKafkaCluster] = for {
          (env, commit) <- note
          rkc <- commit.valueOption
        } yield {
          (env, rkc)
        }
        // Remove goner keys.
        for ((env, things) <- kafkas) {
          if (!flatNote.contains(env)
            || things.config.zkConnect != flatNote(env).zkConnect
            || things.config.zkTimeout != flatNote(env).zkTimeout) {
            kafkasLock {
              val goner = kafkas.remove(env)
              goner.get.close()
            }
          }
        }
        // Add new paths or update existed.
        for ((env, rk) <- flatNote) {
          kafkas(env) = kafkas.get(env).map(_.apply(rk)).getOrElse(Things(rk))
        }
      }
    })
  }

  private def triggerOrResolveIncidentByThreshold(
    value: Double,
    threshold: Double,
    description: String,
    details: => Map[String, Any],
    incidentKey: String,
    pagerKey: Option[String]
  ): Unit = {
    if (value > threshold) {
      triggerIncident(
        None,
        description,
        details ++ Map("threshold" -> threshold, "value" -> value),
        incidentKey,
        pagerKey
      )
    } else {
      resolveIncident(incidentKey, pagerKey)
    }
  }

  private def resolveIncident(incidentKey: String, pagerKey: Option[String]): Unit = {
    if (isCurrentlyFailing.getOrElse(incidentKey, false)) {
      isCurrentlyFailing(incidentKey) = false
      pagerKey.ifDefined { pk => resolvePager(incidentKey, pk) }
    }
  }

  private def triggerIncident(
    throwable: Option[Throwable],
    description: String,
    details: Map[String, Any],
    incidentKey: String,
    pagerKey: Option[String]
  ): Unit = {
    isCurrentlyFailing(incidentKey) = true
    raiseAlert(throwable, Severity.COMPONENT_FAILURE, description, details)
    pagerKey.ifDefined { pk => triggerPager(incidentKey, pk, description, details) }
  }

  private def checkBrokersIds(env: String, things: Things): Unit = {
    val service = "kafka/%s/broker".format(env)

    val ids = things.brokers.brokerMap.keys.map(_.toInt).toSeq
    val pattern = things.config.brokerStartId until things.config.brokersCount + things.config.brokerStartId

    pattern.diff(ids).foreach(
      id => triggerIncident(
        None,
        "Broker is missing: %s".format(service),
        Map("id" -> id),
        "%s/missed/%s".format(service, id),
        things.config.pagerKey
      )
    )

    ids.intersect(pattern)
      .foreach(id => resolveIncident("%s/missed/%s".format(service, id), things.config.pagerKey))

    ids.diff(pattern).foreach(
      id => triggerIncident(
        None,
        "Broker id is out of bounds: %s".format(service),
        Map("id" -> id),
        "%s/outofbound/%s".format(service, id),
        things.config.pagerKey
      )
    )
  }

  override def init(): Unit = {
    // Force initialization.
    closer
  }

  override def probe(): Boolean = {
    dampedHeartbeat()

    kafkasLock {
      for ((env, things) <- kafkas) {
        val service = "kafka/%s/broker".format(env)
        log.info("Probing env[%s]".format(env))

        checkBrokersIds(env, things)

        val underReplicatedFuture: Future[Unit] = pool {
          /**
            * Offline partition is a partition what doesn't have alive replica.
            */
          things.config.thresholds.offlinePartitions.ifDefined { threshold =>
            val offline = things.topicPartitionsState.offline()
            triggerOrResolveIncidentByThreshold(
              offline.size,
              0,
              "Offline partitions: %s".format(service),
              Map("maxOfflinePartition" -> offline.maxBy(_._2.getMillis)),
              "%s/offlinePartitions".format(service),
              things.config.pagerKey
            )
          }

          /**
            * Percent of under-replicated partitions for entire cluster.
            * Partition is under-replicated when it has less in-sync replicas than assigned.
            */
          things.config.thresholds.underReplicatedPartitionsPercentage.ifDefined { threshold =>
            val partitionsCount = things.topicPartitionsState.partitionsCount()
            val underReplicated = things.topicPartitionsState.underReplicated()
            val maxUnderReplicatedOpt = underReplicated.maxByOpt(_._2.getMillis)

            triggerOrResolveIncidentByThreshold(
              (underReplicated.size / partitionsCount.toDouble) * 100,
              threshold,
              "Under-replicated partitions: %s".format(service),
              Map(
                "partitionsCount" -> partitionsCount,
                "underReplicatedCount" -> underReplicated.size,
                "maxUnderReplicatedPartition" -> maxUnderReplicatedOpt.map(_._1).getOrElse("None"),
                "maxUnderReplicatedDuration" ->
                  maxUnderReplicatedOpt.map(max => PeriodUtil.format(new Period(max._2, DateTime.now)))
                    .getOrElse("None")
              ),
              "%s/underReplicated".format(service),
              things.config.pagerKey
            )
          }

          /**
            * Even if small amount of all partitions is under-replicated, but at least 1 partition is under-replicated
            * for some continuous period of time it's an exceptional situation
            */
          things.config.thresholds.underReplicatedPeriod.ifDefined { threshold =>
            val underReplicated = things.topicPartitionsState.underReplicatedSince(DateTime.now - threshold)
            val maxUnderReplicatedOpt = underReplicated.maxByOpt(_._2.getMillis)

            triggerOrResolveIncidentByThreshold(
              underReplicated.size,
              0,
              "Under-replicated partitions for period of time: %s".format(service),
              Map(
                "period" -> threshold,
                "underReplicatedCount" -> underReplicated.size,
                "maxUnderReplicatedPartition" -> maxUnderReplicatedOpt.map(_._1).getOrElse("None"),
                "maxUnderReplicatedDuration" ->
                  maxUnderReplicatedOpt.map(max => PeriodUtil.format(new Period(max._2, DateTime.now)))
                    .getOrElse("None")
              ),
              "%s/underReplicatedForPeriod".format(service),
              things.config.pagerKey
            )
          }
        }

        val futures = for (
          broker <- things.brokers.brokerMap.values.map(b => BrokerEndPoint(b.id.toInt, b.host, b.port)).toSeq
        ) yield {
          pool {
            log.info("Probing env[%s] and broker[#%s (%s)]".format(env, broker.id, broker.host))
            val jmxPort = things.config.jmxPort.getOrElse(defaultJmxPort)

            Jmx.withConnection(broker.host, jmxPort) { connection =>
              val isController = Jmx.getAttribute[Integer](
                connection, "kafka.controller:type=KafkaController,name=ActiveControllerCount", "Value"
              ) > 0

              /**
                * Unclean leader election is called when not in-sync replica became a partition leader.
                *
                * "kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec" JMX metric shows rate
                * of unclean leader election in cluster per second. But it has a lot of different attributes.
                * Attribute "Count" returns count of unclean leader elections from the broker start time.
                * Hence we store the value of the previous check and compare current value with previous.
                *
                * This metric makes sense only for coordinator broker.
                */
              things.config.thresholds.uncleanLeaderElection.ifDefined { threshold =>
                if (isController) {
                  val incidentKey = "%s/unclean".format(service)

                  val current = Jmx.getAttribute[Number](
                    connection,
                    "kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec",
                    "Count"
                  ).doubleValue()

                  //We need this lock because controller could be changed during the check
                  uncleanLock {
                    previousUncleanValues.getOrElse(incidentKey, BrokerValue(None, 0.0)) match {
                      case BrokerValue(Some(previousBrokerId), previousValue) if broker.id != previousBrokerId =>
                        log.info(
                          "Coordinator for env[%s] was changed from [%s] to [%s]."
                            .format(env, previousBrokerId, broker.id)
                        )

                      case BrokerValue(Some(_), previousValue) if current < previousValue =>
                        log.info(
                          ("Current value[%s] of unclean leader elections less than previous[%s] for env[%s] " +
                            "and broker[#%s (%s)].").format(current, previousValue, env, broker.id, broker.host)
                        )

                      case BrokerValue(Some(_), previousValue) if (current - previousValue) > 0 =>
                        triggerIncident(
                          None,
                          "Unclean leader elections occur: %s".format(service),
                          Map("countOfUncleanElections" -> (current - previousValue)),
                          incidentKey,
                          things.config.pagerKey
                        )

                      case _ => //Ignore
                    }

                    previousUncleanValues(incidentKey) = BrokerValue(Some(broker.id), current)
                  }
                }
              }

            } match {
              case Return(_) =>
                log
                  .info("Probing for env[%s] and broker[#%s (%s)] is ended".format(env, broker.id, broker.host))
                resolveIncident("%s/%s/check".format(service, broker.id), things.config.pagerKey)

              case Throw(e) =>
                log.error(
                  e,
                  "Probing for env[%s] and broker[#%s (%s)] finished with error"
                    .format(env, broker.id, broker.host)
                )
                triggerIncident(
                  Some(e),
                  "Kafka broker probing finished with error",
                  Map("env" -> env, "broker" -> broker.host),
                  "%s/%s/check".format(service, broker.id),
                  things.config.pagerKey
                )
            }
          }
        }

        Await.result(
          Future.collect(underReplicatedFuture +: futures) onFailure {
            e => log.error(e, "Error while checking brokers for env[%s]".format(env))
          }
        )
        log.info("Probing for env[%s] is ended".format(env))
      }
    }

    true
  }

  override def close(): Unit = {
    kafkasLock {
      pool.executor.shutdown()
      Await.result(closer.close())
      kafkas.foreach { case (_, things) => things.close() }
    }
  }

  override def label: String = "kafka cluster"
}
