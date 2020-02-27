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

import _root_.scala.collection.JavaConverters._
import _root_.scala.util.control.NonFatal
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.annotations.VisibleForTesting
import com.google.common.io.Closer
import com.google.common.net.HostAndPort
import com.metamx.common.scala.Logging
import com.metamx.common.scala.event.Metric
import com.metamx.common.scala.untyped.Dict
import com.metamx.emitter.core.Event
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.{ServiceEmitter, ServiceMetricEvent}
import com.metamx.health.agent.{HeartbeatingAgent, PeriodicAgent}
import com.metamx.health.mesos.MesosAgent.Deserializer
import com.metamx.health.notify.inventory.NotifyInventory
import com.metamx.rainer.{Commit, CommitKeeper}
import com.twitter.finagle.http.Method.Get
import com.twitter.finagle.http.Version.Http11
import com.twitter.finagle.http.{MediaType, Request, Response, Status}
import com.twitter.finagle.{Http, Service}
import com.twitter.util._
import java.io.InputStream
import java.lang.Double
import java.net.ProtocolException
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths
import org.joda.time.{DateTime, Period}

/**
  * Not to be confused with a Mesos Agent, this is the Health Agent which probes Mesos
  */
class MesosAgent(
  val emitter: ServiceEmitter,
  val pagerInventory: Option[NotifyInventory],
  config: HealthMesosConfig,
  rainerKeeper: CommitKeeper[RainerMesos],
  curatorFrameworkBuilder: CuratorFrameworkBuilder
) extends PeriodicAgent with HeartbeatingAgent with Logging
{
  protected[mesos] val rainers       = new AtomicReference[Map[String, Commit[RainerMesos]]]()
  private lazy     val rainerCloser  = rainerKeeper.mirror().changes.register(Witness(rainers))
  private          val startStopLock = new AnyRef()

  override def period: Period = config.period

  override def probe(): Boolean = {
    dampedHeartbeat()
    val snapshot: Map[String, RainerMesos] = Option(rainers.get()) match {
      case Some(rainerMap) =>
        for {
          (env, commit) <- rainerMap
          envConfig <- commit.value match {
            case Some(Right(x)) =>
              log.debug("Found valid config for environment %s".format(env))
              Some(x)

            case None => None

            case _ =>
              raiseAlert(
                Severity.SERVICE_FAILURE,
                "Bad rainer config for mesos agent environment %s".format(env),
                Map.empty[String, Any]
              )
              None
          }
        } yield {
          (env, envConfig)
        }

      case None =>
        raiseAlert(Severity.SERVICE_FAILURE, "No Rainer Config for mesos agent!", Map.empty[String, Any])
        Map.empty[String, RainerMesos]
    }
    for {
      (env, envConfig) <- snapshot
    } {
      val closer = Closer.create()
      try {
        val cf = closer.register(curatorFrameworkBuilder(envConfig))
        cf.start()
        // We want the timeouts to be handled in CuratorFramework,
        // so wait for "more time than the connection timeout here
        if (!cf.blockUntilConnected(envConfig.zkConnectionTimeout * 5, TimeUnit.MILLISECONDS)) {
          throw new RuntimeException("Unable to connect to Curator for [%s]".format(env))
        }
        val masters = findMasters(cf, envConfig.zkMasterPath).map(_.withEnv(env))
        log.info("Discovered %d masters ", masters.size)
        probeMesosMetrics(masters, MesosAgent.masterDeserializers)
        log.info("Probing mesos agents in %s".format(env))
        probeMesosSlaves(masters)
      } catch {
        case NonFatal(t) =>
          raiseAlert(
            t,
            Severity.SERVICE_FAILURE,
            "Failed checking mesos environment %s".format(env),
            Map.empty[String, Any]
          )
          closer.rethrow(t)
      } finally {
        closer.close()
      }
    }
    true
  }

  private[mesos] def findMasters(cf: CuratorFramework, zkPathPrefix: String): Seq[MesosMasterZkInfo] = {
    cf.getChildren.forPath(zkPathPrefix).asScala.toList
      .withFilter(_.startsWith("json.info_"))
      .flatMap {
        masterJson =>
          val childPath = ZKPaths.makePath(zkPathPrefix, masterJson)
          try {
            Some(
              MesosAgent.defaultMapper
                .readValue(
                  cf.getData.forPath(childPath),
                  classOf[MesosMasterZkInfo]
                )
            )
          } catch {
            case NonFatal(t) =>
              raiseAlert(
                t,
                Severity.COMPONENT_FAILURE,
                "Mesos master ZK parse exception at %s".format(childPath),
                Map.empty[String, Any]
              )
              None
          }
      }
  }

  private[mesos] def probeMesosMetrics(
    nodes: Seq[MetricsDimsServiceEndpoint],
    deserializers: Map[String, Deserializer]
  ): Unit =
  {
    log.info("Probing %s nodes".format(nodes.length))
    val responseFuturePairs = nodes.flatMap {
      node =>
        val metadata = node.metricsDims ++ Map("service" -> Seq(node.service), "host" -> Seq(node.hostAndPort.toString))
        deserializers.map {
          case (path, deserializer) =>
            log.debug("Probing %s for '%s' metrics", node.hostAndPort, path)
            val enhancedHttpResponse = fetch(node, path, metadata)
            val future = enhancedHttpResponse.requestFuture
              .map(deserializeMetrics(deserializer, enhancedHttpResponse, _))
              .onSuccess(_.foreach(emitter.emit))
            (enhancedHttpResponse, future)
        }
    }

    awaitResultOrTimeout(responseFuturePairs)

    responseFuturePairs.foreach(alertOnFailure)

    closeAll(responseFuturePairs)
  }

  private[mesos] def deserializeMetrics(
    deserialize: Deserializer,
    enhancedResponse: EnhancedHttpResponse,
    optionalResponse: Option[Response]
  ): Iterable[ServiceMetricEvent] = optionalResponse match {
    case Some(response) =>
      try {
        val content = response.getInputStream()
        try {
          deserialize(content, enhancedResponse.node)
        } finally {
          try {
            content.close()
          } catch {
            case NonFatal(e) =>
              log.error(e, "Error closing '%s' input stream from %s"
                .format(enhancedResponse.path, enhancedResponse.node.hostAndPort))
          }
        }
      } catch {
        case NonFatal(e) =>
          raiseAlert(
            e,
            Severity.ANOMALY,
            MesosAgent.produceDeserializationAlertMessage(
              enhancedResponse.path,
              enhancedResponse.hostAndPort
            ),
            enhancedResponse.metadata
          )
          Seq.empty
      }

    case None => Seq.empty
  }

  private[this] def awaitResultOrTimeout(futures: Seq[(EnhancedHttpResponse, Future[Iterable[ServiceMetricEvent]])]): Unit = {
    try {
      Await
        .result(
          Future.collectToTry(futures.map(_._2)),
          Duration.fromMilliseconds(config.probeTotalTimeout.toStandardDuration.getMillis)
        )
    } catch {
      case t: TimeoutException =>
        log.error(t, "Timed out waiting for all metrics")
    }
  }

  private[this] def alertOnFailure: ((EnhancedHttpResponse, Future[Iterable[ServiceMetricEvent]])) => Unit = {
    case (response, future) =>
      future.poll match {
        case Some(Return(_)) => log.debug("Successfully finished probing %s/%s", response.hostAndPort, response.path)
        case Some(Throw(_)) => log.debug("Failed to probe %s/%s", response.hostAndPort, response.path)

        case None =>
          raiseAlert(
            Severity.COMPONENT_FAILURE,
            MesosAgent.produceTimeoutAlertMessage(response.hostAndPort, response.path, config),
            response.metadata
          )
          future.raise(new TimeoutException("Failed to finish within %s".format(config.probeTotalTimeout)))
      }
  }

  private[this] def closeAll(responseFuturePair: Seq[(EnhancedHttpResponse, Future[_])]): Unit = {
    responseFuturePair.groupBy {
      case (response, _) =>
        response.client
    }.foreach {
      case (client, Seq((enhancedResponse, future), _*)) =>
        closeService(client, enhancedResponse.hostAndPort)
    }
  }

  private[mesos] def probeMesosSlaves(masters: Seq[MesosMasterZkInfo]): Unit = {
    probeMesosMetrics(getAllSlaves(masters).toSeq, MesosAgent.slaveDeserializers)
  }

  private[mesos] def getAllSlaves(masters: Seq[MesosMasterZkInfo]): Set[MesosSlaveInfo] = {
    masters.map {
      master =>
        val metadata = master.metricsDims ++
          Map("service" -> Seq(master.service), "host" -> Seq(master.hostAndPort.toString))
        val hostAndPort = master.hostAndPort
        val enhancedHttpResponse = fetch(master, MesosAgent.slavesPath, metadata)
        val future = enhancedHttpResponse.requestFuture.map {
          _.map {
            response =>
              try {
                val content = response.getInputStream()
                try {
                  MesosAgent.defaultMapper.readValue(content, classOf[MesosSlavesInfo]).slaves
                    .map(_.withEnv(master.env))
                } finally {
                  try {
                    content.close()
                  } catch {
                    case NonFatal(e) => log
                      .error(e, "Error closing channel buffer from slave list %s".format(hostAndPort))
                  }
                }
              } catch {
                case NonFatal(e) =>
                  raiseAlert(
                    e,
                    Severity.ANOMALY,
                    "Failed to probe %s mesos master for slave at %s/%s"
                      .format(master.env, hostAndPort, MesosAgent.slavesPath),
                    metadata
                  )
                  Seq.empty
              } finally {
                closeService(enhancedHttpResponse.client, enhancedHttpResponse.hostAndPort)
              }
          }
        }.map {
          _.getOrElse(Seq.empty)
        }
        (enhancedHttpResponse, future)
    }.flatMap {
      case (enhancedHttpResponse, future) =>
        try {
          Await.result(future, Duration.fromMilliseconds(config.masterFetchTimeout.toStandardDuration.getMillis))
        } catch {
          case t: TimeoutException =>
            raiseAlert(
              t,
              Severity.COMPONENT_FAILURE,
              "Timed out waiting for list of mesos slaves from %s".format(enhancedHttpResponse.hostAndPort),
              enhancedHttpResponse.metadata
            )
            future.raise(t)
            Seq.empty
          case NonFatal(e) =>
            raiseAlert(
              e,
              Severity.COMPONENT_FAILURE,
              "Unexpected error when probing mesos master for slave list at %s"
                .format(enhancedHttpResponse.hostAndPort),
              enhancedHttpResponse.metadata
            )
            Seq.empty
        }

      case _ => throw new IllegalStateException("Bad values")
    }.toSet
  }

  override def heartbeatPeriodMs: Long = config.heartbeatPeriod.toStandardDuration.getMillis

  override def init(): Unit = {
    // eager init of closer stuff
    rainerCloser
  }

  override def close(): Unit = {
    startStopLock.synchronized {
      Await.result(
        rainerCloser.close() onFailure {
          t => log.error(t, "Error closing Rainer")
        }
      )
      rainers.set(null)
    }
  }

  private def fetch(
    node: MetricsDimsServiceEndpoint,
    path: String,
    alertData: Dict = Map.empty
  ): EnhancedHttpResponse = {
    val client = fetchClient(node.hostAndPort)
    val request = Request(Http11, Get, MesosAgent.createUri(node.hostAndPort, path))
    request.headerMap.set("Accepts", MediaType.Json)
    new EnhancedHttpResponse(
      ensureOKResponse(
        futureRequest(client, request).onFailure {
          t =>
            raiseAlert(
              t,
              Severity.ANOMALY,
              MesosAgent.produceFetchAlertMessage(path, node.hostAndPort),
              alertData
            )
        }, node.hostAndPort, path, alertData
      ),
      node,
      path,
      alertData,
      client
    )
  }

  @VisibleForTesting
  private[mesos] def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
    Http.newService(hostAndPort.toString)
  }

  // Unfortunately, the client needs to be closed AFTER the results have been processed.
  // TODO: figure out a way to make this closing more in-the-loop and less prone to be forgotten/missed
  private[mesos] def closeService[Req, Rep](
    client: Service[Req, Rep],
    hostAndPort: HostAndPort,
    waitForFinish: Boolean = true
  ): Unit =
  {
    val future = client.close() onFailure {
      x => {
        raiseAlert(
          x,
          Severity.ANOMALY,
          "Error closing connection to mesos service at %s".format(hostAndPort),
          Map[String, AnyRef]()
        )
      }
    } onSuccess {
      _ => {
        log.trace("Finished closing %s".format(hostAndPort))
      }
    }
    if (waitForFinish) {
      try {
        Await.ready(future, Duration.fromMilliseconds(config.closeTimeout.toStandardDuration.getMillis))
      }
      catch {
        case t: TimeoutException => future.raise(t)
      }
    }
  }

  private[mesos] def ensureOKResponse(
    responseFuture: Future[Response],
    hostAndPort: HostAndPort,
    path: String,
    metadata: Map[String, Any]
  ): Future[Option[Response]] =
  {
    responseFuture map {
      response =>
        try {
          response.status match {
            case Status.Ok => Some(response)
            case Status.TemporaryRedirect =>
              log.info("Redirect from %s ignoring since we should ping the redirect directly.".format(hostAndPort))
              None
            case other: Status =>
              val msg = response.getContentString()
              throw new ProtocolException(
                "Error fetching data on %s/%s : %s : %s".format(hostAndPort, path, other, msg)
              )
          }
        }
        catch {
          case NonFatal(e) =>
            raiseAlert(
              e,
              Severity.COMPONENT_FAILURE,
              MesosAgent.produceBadResponseMessage(hostAndPort),
              metadata
            )
            None
        }
    }
  }

  @VisibleForTesting
  private[mesos] def futureRequest(
    client: Service[Request, Response],
    httpRequest: Request
  ): Future[Response] =
  {
    client(httpRequest)
  }

  override def label: String = "mesos cluster"
}

object MesosAgent
{
  private[mesos] val snapshotPath   = "metrics/snapshot"
  private[mesos] val agentExecutorStatsPath = "monitor/statistics"
  private[mesos] val agentContainerStatsPath = "containers"
  private[mesos] val slavesPath     = "slaves"
  private[mesos] val metricTypeRef  = new TypeReference[Map[String, Object]] {}
  private[mesos] val defaultMapper  = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(SerializationFeature.INDENT_OUTPUT, false)
    .configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false)

  private[mesos] type Deserializer = (InputStream, MetricsDimsServiceEndpoint) => Iterable[ServiceMetricEvent]

  /*
    The original goal with this was because if you happened to hit the mesos master that wasn't leading,
    it would return all 0's for a bunch of metrics, but not all of them. This method is used to enforce
    "Zero counted as missing" with some skew for rounding errors in floating point. Otherwise when an average
    was taken, it would average the zeros in with the non zeros, which would yield the wrong results.
   */
  private[this] val masterMetricsFilter: ((String, Any)) => Boolean = {
    case (_, d: Double) => Math.abs(d) > 1e-6 // mostly not zero
    case _ => false
  }

  private[mesos] val masterDeserializers: Map[String, Deserializer] = Map(
    MesosAgent.snapshotPath -> MesosAgent.deserializeSnapshotMetrics(masterMetricsFilter)
  )

  private[mesos] val slaveDeserializers: Map[String, Deserializer] = Map(
    MesosAgent.snapshotPath -> MesosAgent.deserializeSnapshotMetrics(_ => true),
    MesosAgent.agentExecutorStatsPath -> MesosAgent.deserializeExecutorMetrics,
    MesosAgent.agentContainerStatsPath -> MesosAgent.deserializeContainerMetrics
  )

  def createUri(hostAndPort: HostAndPort, path: String): String = {
    "http://%s/%s".format(hostAndPort, path)
  }

  def deserializeSnapshotMetrics(
    metricsFilter: ((String, Any)) => Boolean
  ) (content: InputStream, node: MetricsDimsServiceEndpoint): Iterable[ServiceMetricEvent] =
  {
    // Timestamp is at END of PROBE of node
    val timestamp = new DateTime()

    defaultMapper.readValue[Dict](content, MesosAgent.metricTypeRef)
      .withFilter(metricsFilter)
      .map {
        case (metric, value) =>
          Metric(
            metric,
            value = value.asInstanceOf[Double],
            userDims = node.metricsDims,
            created = timestamp
          ).build(node.service, node.hostAndPort.toString)
      }
  }

  def deserializeExecutorMetrics(stream: InputStream, node: MetricsDimsServiceEndpoint): Seq[ServiceMetricEvent] = {
    deserializeAgentNodeMetrics[MesosExecutorInfo](stream, node, new TypeReference[Seq[MesosExecutorInfo]] {})
  }

  def deserializeContainerMetrics(stream: InputStream, node: MetricsDimsServiceEndpoint): Seq[ServiceMetricEvent] = {
    deserializeAgentNodeMetrics[MesosContainerInfo](stream, node, new TypeReference[Seq[MesosContainerInfo]] {})
  }

  private def deserializeAgentNodeMetrics[Info <: MesosAgentStatistics](
    stream: InputStream,
    node: MetricsDimsServiceEndpoint,
    typeReference : TypeReference[Seq[Info]]
  ) = {
    val timestamp = new DateTime()
    val infos = defaultMapper.readValue[Seq[Info]](stream, typeReference)

    infos.flatMap(
      (info: Info) => info.statistics
        .withFilter {
          case ("timestamp", _) => false
          case _ => true
        }
        .map {
          case (metric, value) =>
            Metric(
              metric = "mesos/%s/%s".format(info.label, metric),
              value,
              userDims = node.metricsDims ++ info.metricsDims,
              created = timestamp
            ).build(node.service, node.hostAndPort.toString)
        }
    )
  }

  def produceBadResponseMessage(hostAndPort: HostAndPort): String = {
    "Failed to fetch response from mesos node %s".format(hostAndPort)
  }
  def produceDeserializationAlertMessage(path: String, port: HostAndPort): String = {
    "Failed to deserialize metrics for mesos %s/%s".format(port, path)
  }

  def produceFetchAlertMessage(path: String, port: HostAndPort): String = {
    "Error fetching mesos data from %s/%s".format(port, path)
  }

  def produceTimeoutAlertMessage(port: HostAndPort, path: String, config: HealthMesosConfig): String = {
    "Timed out waiting %s ms for result from mesos node at %s/%s".format(
      config.probeTotalTimeout.toStandardDuration.getMillis.toString,
      port,
      path
    )
  }
}

case class MesosSlavesInfo(
  slaves: Seq[MesosSlaveInfo]
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class MesosExecutorInfo(
  executor_id: String,
  framework_id: String,
  source: String,
  statistics: Map[String, Number]
) extends MesosAgentStatistics
{
  override def metricsDims = {
     Map(
       "executor_id" -> Seq(executor_id),
       "framework_id" -> Seq(framework_id),
       "executor_source" -> Seq(source)
     )
  }

  override def label: String = "executor"
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class MesosContainerInfo(
  container_id: String,
  executor_id: String,
  framework_id: String,
  source: String,
  statistics: Map[String, Number],
  status: Map[String, Object]
) extends MesosAgentStatistics
{
  override def metricsDims = {
    Map(
      "container_id" -> Seq(container_id),
      "executor_id" -> Seq(executor_id),
      "framework_id" -> Seq(framework_id),
      "source" -> Seq(source),
      "executor_pid" -> Seq(status.getOrElse("executor_pid", -1).toString)
    )
  }

  override def label: String = "container"
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class MesosSlaveInfo(
  id: String,
  pid: String,
  version: String,
  env: String = "unknown"
) extends MetricsDimsServiceEndpoint with Comparable[MesosSlaveInfo]
{
  def withEnv(env: String): MesosSlaveInfo = {
    copy(env = env)
  }

  override def hostAndPort: HostAndPort = {
    val splits = pid.split("@")
    if (splits.length != 2) {
      throw new IllegalArgumentException("Error parsing `%s`, Expected two sides of a `@`".format(pid))
    }
    HostAndPort.fromString(splits(1))
  }

  override def metricsDims = {
    Map[String, Iterable[String]](
      "port" -> Seq(hostAndPort.getPort.toString),
      "id" -> Seq(id),
      "version" -> Seq(version)
    )
  }

  override def service = "mesos/%s/agent".format(env)

  override def compareTo(o: MesosSlaveInfo): Int = id.compareTo(o.id)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class MesosMasterZkInfo(
  hostname: String,
  id: String,
  port: Int,
  version: String,
  env: String = "unknown"
) extends MetricsDimsServiceEndpoint
{

  def withEnv(env: String): MesosMasterZkInfo = {
    copy(env = env)
  }

  override def metricsDims = Map(
    "version" -> Seq(version),
    "id" -> Seq(id),
    "port" -> Seq(port.toString)
  )

  override def hostAndPort: HostAndPort = HostAndPort.fromParts(hostname, port)

  override def service: String = "mesos/%s/master".format(env)
}

class MesosMasterEvent(
  map: util.Map[String, AnyRef],
  timestamp: DateTime,
  feed: String
) extends Event
{
  override def toMap: util.Map[String, AnyRef] = map

  override def getFeed: String = feed

  override def getCreatedTime: DateTime = timestamp

  override def isSafeToBuffer: Boolean = true
}

class EnhancedHttpResponse(
  val requestFuture: Future[Option[Response]],
  val node: MetricsDimsServiceEndpoint,
  val path: String,
  val metadata: Map[String, Any],
  val client: Service[Request, Response]
)
{
  val hostAndPort = node.hostAndPort
}


trait MetricsDimsServiceEndpoint extends MetricsDims with ServiceEndpoint

trait MetricsDims
{
  def metricsDims: Map[String, Iterable[String]]
}

trait MesosAgentStatistics extends MetricsDims
{
  def statistics: Map[String, Number]

  def label: String
}

trait ServiceEndpoint
{
  def service: String

  def hostAndPort: HostAndPort
}
