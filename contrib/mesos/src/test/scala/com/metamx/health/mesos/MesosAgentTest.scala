package com.metamx.health.mesos

import _root_.scala.io.Source
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.net.HostAndPort
import com.metamx.common.scala.Predef.EffectOps
import com.metamx.emitter.core.{Emitter, Event}
import com.metamx.emitter.service.{AlertEvent, ServiceEmitter, ServiceMetricEvent}
import com.metamx.rainer.{Commit, CommitKeeper}
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.io.Buf.ByteArray
import com.twitter.util.Awaitable.CanAwait
import com.twitter.util.{Await, Future, Time, TimeoutException, Try, Var, Duration => TwitterDuration}
import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{GetChildrenBuilder, GetDataBuilder}
import org.apache.curator.utils.ZKPaths
import org.apache.log4j.BasicConfigurator
import org.jboss.netty.buffer.{ChannelBufferInputStream, LittleEndianHeapChannelBuffer}
import org.joda.time.{Duration, Period}
import org.junit.runner.RunWith
import org.scalamock.matchers.{ArgThat, MockParameter}
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.junit.JUnitRunner
import org.scalatest.time._
import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class MesosAgentTest extends FlatSpec with Matchers with MockFactory with TimeLimitedTests
{
  BasicConfigurator.configure()

  val config = new HealthMesosConfig {
    override def heartbeatPeriod: Period = Period.ZERO

    override def period: Period = Period.ZERO

    override def autoPublishDuration: Duration = Duration.ZERO

    override def autoPublishFuzz: Double = 0.0

    override def zkPath: String = ""

    override def table: String = ""

    override def probeTotalTimeout: Period = Period.seconds(100)

    override def masterFetchTimeout: Period = Period.seconds(100)

    override def closeTimeout: Period = Period.seconds(100)
  }

  val lowTimeoutConfig = new HealthMesosConfig {
    override def heartbeatPeriod: Period = Period.ZERO

    override def period: Period = Period.ZERO

    override def autoPublishDuration: Duration = Duration.ZERO

    override def autoPublishFuzz: Double = 0.0

    override def zkPath: String = ""

    override def table: String = ""

    override def probeTotalTimeout: Period = Period.millis(10)

    override def masterFetchTimeout: Period = Period.millis(10)

    override def closeTimeout: Period = Period.millis(10)
  }

  val rainerCf     = mock[CuratorFramework]
  val rainerPath   = "some/path"
  val rainerKeeper = new CommitKeeper[RainerMesos](rainerCf, rainerPath) {
    override def mirror(): Var[Map[Commit.Key, Commit[RainerMesos]]] = Var(Map.empty)
  }

  val fakeService  = "mesos/junit/service"
  val masterZkPath = "some/master/path"
  val exhibitorURI = new URI("http://localhost:83819")
  val cf           = mock[CuratorFramework]
  val cfBuilder    = new CuratorFrameworkBuilder("default:1234") {
    override def apply(rainerMesos: RainerMesos): CuratorFramework =
    {
      // Test the test... make sure we don't feed in something unexpected
      rainerMesos.exhibitorURI match {
        case Some(uri) if uri.equals(exhibitorURI) => cf
        case None if rainerMesos.zkMasterPath.equals(masterZkPath) => cf
      }
    }
  }
  val mapper       = new ObjectMapper().registerModule(DefaultScalaModule)

  val slaveSnapshotMetricsCount = 50
  val executorCount             = 11
  val metricsPerExecutor        = 14
  val containerCount            = 2
  val metricsPerContainer       = 14

  "MesosAgent.init" should "properly init on empty configs" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    agent.init()
  }

  "MesosAgent.probe" should "properly probe nothing on empty configs" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    emitter.emit _ expects new MockParameter[Event](
      new ArgThat[Event](
        {
          case event: Event =>
            event.toMap.containsKey("feed") &&
              "heartbeats".equals(event.toMap.get("feed"))
          case _ => false
        }
      )
    )
    agent.rainers.set(Map.empty)
    agent.probe() should be(true)
  }

  "MesosAgent.probe" should "alert if no config" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    emitter.emit _ expects new MockParameter[Event](
      new ArgThat[Event](
        {
          case event: Event =>
            event.toMap.containsKey("feed") &&
              "heartbeats".equals(event.toMap.get("feed"))
          case _ => false
        }
      )
    )

    emitter.emit _ expects new MockParameter[Event](
      new ArgThat[Event](
        {
          case event: Event =>
            event.toMap.containsKey("description")
          case _ => false
        }
      )
    )

    agent.probe() should be(true)
  }

  "MesosAgent.findMasters" should "find masters" in {
    val json_info = "json.info_1"
    val childGetBuilder = mock[GetChildrenBuilder]
    val getDataBuilder = mock[GetDataBuilder]
    (cf.getChildren _).expects().returns(childGetBuilder)
    (childGetBuilder.forPath _).expects(masterZkPath).returns(List[String](json_info).asJava)
    (cf.getData _).expects().returns(getDataBuilder)
    (getDataBuilder.forPath _).expects(ZKPaths.makePath(masterZkPath, json_info))
      .returns(MesosAgentTest.MASTER_ZK_ENTRY)
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    val masters = agent.findMasters(cf, masterZkPath)
    masters.size should be(1)
    val master = masters.head
    master.hostname should equal("mesos-master.example.com.")
    master.id should equal("c780d5cb-f148-48a4-b37c-9dfaac2f2161")
    master.port should equal(5050)
    master.version should equal("0.28.0")
  }

  "MesosAgent.getAllSlaves" should "find slaves properly" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val fakeHostAndPort = HostAndPort.fromParts("some_host", 5432)

    val clientService = new CountingConstantService(
      Map(MesosAgent.createUri(fakeHostAndPort, "slaves") -> okResponseGenerator(MesosAgentTest.SLAVES))
    )

    val masterId = "some_master_id"
    val masterVersion = "someVersion"

    val slaves = Set[HostAndPort]() ++ mapper.readValue(MesosAgentTest.SLAVES, classOf[MesosSlavesInfo]).slaves.map(
      _.hostAndPort
    )

    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    {
      override def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
        if (hostAndPort.equals(fakeHostAndPort)) {
          clientService
        } else {
          throw new IllegalStateException("Bad host %s".format(hostAndPort))
        }
      }
    }

    agent
      .getAllSlaves(
        Seq(
          MesosMasterZkInfo(
            fakeHostAndPort.getHostText,
            masterId,
            fakeHostAndPort.getPort,
            masterVersion
          )
        )
      ).map(_.hostAndPort) should equal(slaves)
    clientService.closes should be(1)
    clientService.assertRequestCount(fakeHostAndPort, "slaves", 1)
  }

  "MesosAgent.getAllSlaves" should "ignore bad slave results" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val fakeHostAndPort = HostAndPort.fromParts("some_host", 5432)

    (emitter.emit _).expects(
      new MockParameter[Event](
        new ArgThat[Event](
          {
            case event: Event =>
              event.toMap.containsKey("description") &&
                "Failed to probe unknown mesos master for slave at some_host:5432/slaves"
                  .equals(event.toMap.get("description"))
            case _ => false
          }
        )
      )
    ).once()

    val clientService = new CountingConstantService(
      Map(MesosAgent.createUri(fakeHostAndPort, "slaves") -> okResponseGenerator(createErroneousBytes))
    )

    val masterId = "some_master_id"
    val masterVersion = "someVersion"

    val slaves = Set[HostAndPort]() ++ mapper.readValue(MesosAgentTest.SLAVES, classOf[MesosSlavesInfo]).slaves.map(
      _.hostAndPort
    )

    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    {
      override def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
        if (hostAndPort.equals(fakeHostAndPort)) {
          clientService
        } else {
          throw new IllegalStateException("Bad host %s".format(hostAndPort))
        }
      }
    }

    agent
      .getAllSlaves(
        Seq(
          MesosMasterZkInfo(
            fakeHostAndPort.getHostText,
            masterId,
            fakeHostAndPort.getPort,
            masterVersion
          )
        )
      ).map(_.hostAndPort) shouldBe 'Empty
    clientService.closes should be(1)
    clientService.assertRequestCount(fakeHostAndPort, "slaves", 1)
  }

  "MesosAgent" should "ignore timed out masters" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val fakeHostAndPort = HostAndPort.fromParts("some_host", 5432)

    (emitter.emit _).expects(
      where {
        event: Event =>
          event.toMap.containsKey("description") &&
            "Timed out waiting for list of mesos slaves from some_host:5432".equals(event.toMap.get("description"))
      }
    ).once()

    val clientService = new CountingConstantService(
      Map(MesosAgent.createUri(fakeHostAndPort, "slaves") -> okResponseGenerator(Array()))
    )
    val masterId = "some_master_id"
    val masterVersion = "someVersion"

    val slaves = Set[HostAndPort]() ++ mapper.readValue(MesosAgentTest.SLAVES, classOf[MesosSlavesInfo]).slaves.map(
      _.hostAndPort
    )

    val agent = new MesosAgent(
      serviceEmitter,
      None,
      lowTimeoutConfig,
      rainerKeeper,
      cfBuilder
    )
    {
      override def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
        if (hostAndPort.equals(fakeHostAndPort)) {
          clientService
        } else {
          throw new IllegalStateException("Bad host %s".format(hostAndPort))
        }
      }

      override def futureRequest(
        client: Service[Request, Response],
        httpRequest: Request
      ): Future[Response] =
      {
        new FreezingFuture[Response]
      }
    }

    agent
      .getAllSlaves(
        Seq(
          MesosMasterZkInfo(
            fakeHostAndPort.getHostText,
            masterId,
            fakeHostAndPort.getPort,
            masterVersion
          )
        )
      ).map(_.hostAndPort) shouldBe 'Empty
    clientService.closes should be(0)
    clientService.assertRequestCount(fakeHostAndPort, "slaves", 0)
  }

  "MesosAgent.getAllSlaves" should "find slaves only once" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val fakeHostAndPort = HostAndPort.fromParts("some_host", 5432)

    val clientService = new CountingConstantService(
      Map(MesosAgent.createUri(fakeHostAndPort, "slaves") -> okResponseGenerator(MesosAgentTest.SLAVES))
    )

    val masterId = "some_master_id"
    val masterVersion = "someVersion"

    val slaves = Set[HostAndPort]() ++ mapper.readValue(MesosAgentTest.SLAVES, classOf[MesosSlavesInfo]).slaves.map(
      _.hostAndPort
    )
    val masterInfo = MesosMasterZkInfo(
      fakeHostAndPort.getHostText,
      masterId,
      fakeHostAndPort.getPort,
      masterVersion
    )

    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    {
      override def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
        if (hostAndPort.equals(fakeHostAndPort)) {
          clientService
        } else {
          throw new IllegalStateException("Bad host %s".format(hostAndPort))
        }
      }
    }

    agent
      .getAllSlaves(
        Seq(
          masterInfo,
          masterInfo
        )
      ).map(_.hostAndPort) should equal(slaves)
    clientService.closes should be(2)
    clientService.assertRequestCount(fakeHostAndPort, "slaves", 2)
  }

  "MesosAgent.probeMesosMetricsSnapshot" should "transparently timeout on metrics snapshot" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val frozenFuture = new FreezingFuture[Response]
    val fakeHostAndPort = HostAndPort.fromParts("some_host", 5432)

    // Shouldn't ever be called but helps shake out bad tests
    val response = () => Response().withEffect(_.contentString = "{}")

    (emitter.emit _).expects(
      new MockParameter[Event](
        new ArgThat[Event](
          {
            case event: Event =>
              event.toMap.containsKey("description") &&
                "Timed out waiting 10 ms for result from mesos node at some_host:5432/metrics/snapshot"
                  .equals(event.toMap.get("description"))
            case _ => false
          }
        )
      )
    ).once()

    val clientService = new CountingConstantService(
      Map(MesosAgent.createUri(fakeHostAndPort, "metrics/snapshot") -> response)
    )

    val agent = new MesosAgent(
      serviceEmitter,
      None,
      lowTimeoutConfig,
      rainerKeeper,
      cfBuilder
    )
    {
      override def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
        if (hostAndPort.equals(fakeHostAndPort)) {
          clientService
        } else {
          throw new IllegalStateException("Bad host %s".format(hostAndPort))
        }
      }

      override def futureRequest(
        client: Service[Request, Response],
        httpRequest: Request
      ): Future[Response] =
      {
        frozenFuture
      }
    }

    agent.probeMesosMetrics(
      Seq(createEndpointDims(fakeService, Map.empty, fakeHostAndPort)),
      MesosAgent.masterDeserializers
    )

    frozenFuture.raised shouldBe 'Defined
    frozenFuture.raised.get.getClass should be(classOf[TimeoutException])
    clientService.closes should be(1)
    clientService.assertRequestCount(fakeHostAndPort, "metrics/snapshot", 0)
  }

  "MesosAgent.probeMesosMetrics" should "process slave snapshot properly" in {
    val emitter = mock[Emitter]
    val other_host = "other_host"
    val other_service = "other_service"
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)

    val metricDimMap = Map[String, Iterable[String]]("dim1" -> Seq("val1"))
    val metricValMap: Map[String, Object] = mapper.readValue(MesosAgentTest.SLAVE_SNAPSHOT, MesosAgent.metricTypeRef)

    val fakeHostAndPort = HostAndPort.fromParts(other_host, 5432)

    (emitter.emit _).expects(
      new MockParameter[Event](
        new ArgThat[Event](
          {
            case event: Event =>
              metricValMap.contains(event.toMap.get("metric").toString) &&
                metricDimMap.keys.forall(event.toMap.containsKey) &&
                fakeHostAndPort.toString.equals(event.toMap.get("host")) &&
                other_service.equals(event.toMap.get("service"))
            case _ => false
          }
        )
      )
    ).repeat(slaveSnapshotMetricsCount).times()

    val clientService = new CountingConstantService(
      Map(MesosAgent.createUri(fakeHostAndPort, "metrics/snapshot") -> okResponseGenerator(MesosAgentTest.SLAVE_SNAPSHOT))
    )

    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    {
      override def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
        if (hostAndPort.equals(fakeHostAndPort)) {
          clientService
        } else {
          throw new IllegalStateException("Bad host %s".format(hostAndPort))
        }
      }
    }

    agent.probeMesosMetrics(
      Seq(createEndpointDims(other_service, metricDimMap, fakeHostAndPort)),
      Map(MesosAgent.snapshotPath -> MesosAgent.deserializeSnapshotMetrics(_ => true))
    )
    clientService.closes should be(1)
    clientService.assertRequestCount(fakeHostAndPort, "metrics/snapshot", 1)
  }

  "MesosAgent.probeMesosMetrics" should "ignore bad responses" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)

    val metricDimMap = Map[String, Iterable[String]]("dim1" -> Seq("val1"))
    val fakeHostAndPort = HostAndPort.fromParts("some_host", 5432)

    // Emits a notice of the 500 error
    (emitter.emit _).expects(
      where {
        event: Event =>
          event.toMap.asScala("description") == MesosAgent.produceBadResponseMessage(fakeHostAndPort)
      }
    ).once()

    val clientService = new CountingConstantService(
      Map(MesosAgent.createUri(fakeHostAndPort, "metrics/snapshot") -> errorResponseGenerator)
    )

    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    {
      override def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
        if (hostAndPort.equals(fakeHostAndPort)) {
          clientService
        } else {
          throw new IllegalStateException("Bad host %s".format(hostAndPort))
        }
      }
    }

    agent.probeMesosMetrics(
      Seq(createEndpointDims(fakeService, metricDimMap, fakeHostAndPort)),
      MesosAgent.masterDeserializers
    )
    clientService.closes should be(1)
    clientService.assertRequestCount(fakeHostAndPort, "metrics/snapshot", 1)
  }

  "Deserializer" should "parse agent executor statistics" in {
    val stream: InputStream = new ByteArrayInputStream(MesosAgentTest.EXECUTOR_STATISTICS)
    val serviceName = "mesos/1/slave"
    val address = "host:5050"
    val node: MetricsDimsServiceEndpoint = createEndpointDims(
      serviceName,
      Map("dim" -> Seq("val")),
      HostAndPort.fromString(address)
    )
    val metrics: Seq[ServiceMetricEvent] = MesosAgent.deserializeExecutorMetrics(stream, node)

    // sanity check
    assert(metrics.size > 1)
    assert(metrics.head.getService == serviceName)
    assert(metrics.head.getHost == address)

    // check time
    assert(metrics.head.getCreatedTime != null)

    // check dims
    val dims = metrics.head.getUserDims.asScala
    assert(dims("executor_id") != null)
    assert(dims("executor_source") != null)
    assert(dims("framework_id") != null)

    //check dims uniqueness
    assert(metrics.map(_.getUserDims.get("executor_id")).toSet.size > 1)
    assert(metrics.map(_.getUserDims.get("executor_source")).toSet.size > 1)

    // check filter
    assert(!metrics.exists(_.getMetric == "timestamp"))

    // check values
    val map: Map[String, ServiceMetricEvent] = metrics.map(m => m.getMetric -> m).toMap
    def number(metrics : Map[String, ServiceMetricEvent], label : String) = {
      metrics(label).getValue.longValue()
    }
    assert(number(map, "mesos/executor/cpus_limit") > 0)
    assert(number(map, "mesos/executor/cpus_system_time_secs") > 0)
    assert(number(map, "mesos/executor/mem_file_bytes") > 0)

    // check uniqueness
    assert(metrics.filter(_.getMetric == "mesos/executor/cpus_system_time_secs").map(_.getValue).toSet.size > 1)
    assert(metrics.filter(_.getMetric == "mesos/executor/mem_file_bytes").map(_.getValue).toSet.size > 1)
  }

  "Deserializer" should "parse agent container statistics" in {
    val stream: InputStream = new ByteArrayInputStream(MesosAgentTest.CONTAINER_STATISTICS)
    val serviceName = "mesos/1/slave"
    val address = "host:5050"
    val node: MetricsDimsServiceEndpoint = createEndpointDims(
      serviceName,
      Map("dim" -> Seq("val")),
      HostAndPort.fromString(address)
    )
    val metrics: Seq[ServiceMetricEvent] = MesosAgent.deserializeContainerMetrics(stream, node)

    // sanity check
    assert(metrics.size > 1)
    assert(metrics.head.getService == serviceName)
    assert(metrics.head.getHost == address)

    // check time
    assert(metrics.head.getCreatedTime != null)

    // check dims
    val dims = metrics.head.getUserDims.asScala
    var tags = Seq("executor_id", "source", "framework_id", "container_id", "executor_pid")
    for {s <- tags} {
      assert(dims(s) != null, s"No '$s' dimension")
    }

    //check dims uniqueness
    for {s <- tags} {
      assert(metrics.map(_.getUserDims.get(s)).toSet.size > 1, s"All '$s' values are the same")
    }

    // check filter
    assert(!metrics.exists(_.getMetric == "timestamp"))

    // check values
    val map: Map[String, ServiceMetricEvent] = metrics.map(m => m.getMetric -> m).toMap
    def number(metrics : Map[String, ServiceMetricEvent], label : String) = {
      metrics(label).getValue.longValue()
    }
    assert(number(map, "mesos/container/cpus_limit") > 0)
    assert(number(map, "mesos/container/cpus_system_time_secs") > 0)
    assert(number(map, "mesos/container/mem_file_bytes") > 0)

    // check uniqueness
    assert(metrics.filter(_.getMetric == "mesos/container/cpus_system_time_secs").map(_.getValue).toSet.size > 1)
    assert(metrics.filter(_.getMetric == "mesos/container/mem_file_bytes").map(_.getValue).toSet.size > 1)
  }

  "MesosAgent.probeMesosMetrics" should "emit proper number of events for the slave" in {
    val emitter: Emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "sink", emitter)
    val hostAndPort: HostAndPort = HostAndPort.fromString("host:5050")

    (emitter.emit _).expects(where { event: Event => true })
      .repeated(slaveSnapshotMetricsCount + metricsPerExecutor*executorCount + metricsPerContainer*containerCount).times()


    val clientService = new CountingConstantService(
      Map(
        MesosAgent.createUri(hostAndPort, "metrics/snapshot") -> okResponseGenerator(MesosAgentTest.SLAVE_SNAPSHOT),
        MesosAgent.createUri(hostAndPort, "monitor/statistics") -> okResponseGenerator(MesosAgentTest.EXECUTOR_STATISTICS),
        MesosAgent.createUri(hostAndPort, "containers") -> okResponseGenerator(MesosAgentTest.CONTAINER_STATISTICS)
      )
    )
    val agent = createAgent(serviceEmitter, hostAndPort, clientService)

    agent.probeMesosMetrics(Seq(createEndpointDims("", Map.empty, hostAndPort)), MesosAgent.slaveDeserializers)

    clientService.closes should be (1)
    clientService.assertRequestCount(hostAndPort, "metrics/snapshot", 1)
    clientService.assertRequestCount(hostAndPort, "monitor/statistics", 1)
    clientService.assertRequestCount(hostAndPort, "containers", 1)
  }

  "probe" should "alert and proceed on statistics endpoints bad response" in {
    val (emitter, closeService, probe, hostAndPort) = createSlaveProbeEnvironment(
      okResponseGenerator(MesosAgentTest.SLAVE_SNAPSHOT),
      errorResponseGenerator,
      errorResponseGenerator
    )

    val message = MesosAgent.produceBadResponseMessage(hostAndPort)
    (emitter.emit _).expects(where { event: Event => true })
      .repeated(slaveSnapshotMetricsCount).times()
    (emitter.emit _).expects(argThat[AlertEvent] { event => event.getDescription == message })
      .twice() // +2 alert events

    probe()
    closeService.closes should be (1)
    val (_, uris) = environmentHostAndUris
    uris.zip(Seq(1, 1, 1)).foreach {
      case (uri, expectedRequestCount) => closeService.assertRequestCount(uri, expectedRequestCount)
    }
  }

  "probe" should "alert and proceed on a snapshot endpoint bad response" in {
    val (emitter, closeService, probe, hostAndPort) = createSlaveProbeEnvironment(
      errorResponseGenerator,
      okResponseGenerator(MesosAgentTest.EXECUTOR_STATISTICS),
      okResponseGenerator(MesosAgentTest.CONTAINER_STATISTICS)
    )

    val message = MesosAgent.produceBadResponseMessage(hostAndPort)
    (emitter.emit _).expects(argThat[AlertEvent] { event => event.getDescription == message })
      .once()
    (emitter.emit _).expects(where { event: Event => true })
      .repeated(executorCount*metricsPerExecutor + containerCount*metricsPerContainer).times() // +1 alert event

    probe()
    closeService.closes should be (1)
    val (_, uris) = environmentHostAndUris
    uris.zip(Seq(1, 1, 1)).foreach {
      case (uri, expectedRequestCount) => closeService.assertRequestCount(uri, expectedRequestCount)
    }
  }

  "probe" should "alert and proceed on statistics endpoint timeout" in {
    val (emitter, closeService, probe, hostAndPort) = createFreezingEnvironment(
      MesosAgent.snapshotPath,
      okResponseGenerator(MesosAgentTest.SLAVE_SNAPSHOT)
    )

    (emitter.emit _).expects(where { event: Event => true })
      .repeated(slaveSnapshotMetricsCount).times()

    (emitter.emit _).expects(argThat[AlertEvent] { event =>
        event.getDescription == produceTimeoutAlertMessage(hostAndPort, MesosAgent.agentExecutorStatsPath)
    }).once() // +1 timeout alert

    (emitter.emit _).expects(argThat[AlertEvent] { event =>
      event.getDescription == produceTimeoutAlertMessage(hostAndPort, MesosAgent.agentContainerStatsPath)
    }).once() // +1 timeout alert

    probe()
    closeService.closes should be (1)
    val (_, uris) = environmentHostAndUris(Seq(MesosAgent.snapshotPath))
    closeService.assertRequestCount(uris.head, 1)
  }

  private def produceTimeoutAlertMessage(hostAndPort: HostAndPort, path: String) = {
    MesosAgent.produceTimeoutAlertMessage(hostAndPort, path, lowTimeoutConfig)
  }

  "probe" should "alert and proceed on snapshot endpoint timeout" in {
    val (emitter, closeService, probe, hostAndPort) = createFreezingEnvironment(
      Map(
        MesosAgent.agentExecutorStatsPath -> okResponseGenerator(MesosAgentTest.EXECUTOR_STATISTICS),
        MesosAgent.agentContainerStatsPath -> okResponseGenerator(MesosAgentTest.CONTAINER_STATISTICS)
      )
    )

    val message = produceTimeoutAlertMessage(hostAndPort, MesosAgent.snapshotPath)

    (emitter.emit _).expects(where { event: Event => true })
      .repeated(executorCount * metricsPerExecutor + containerCount * metricsPerContainer).times()
    (emitter.emit _).expects(argThat[AlertEvent] { event => event.getDescription == message })
      .once()

    probe()
    closeService.closes should be (1)
    val (_, uris) = environmentHostAndUris(Seq(MesosAgent.agentExecutorStatsPath, MesosAgent.agentContainerStatsPath))
    closeService.assertRequestCount(uris.head, 1)
  }

  "probe" should "alert on endpoint timeout and parse exception separately" in {
    val (emitter, closeService, probe, hostAndPort) = createFreezingEnvironment(
      Map(
        MesosAgent.agentExecutorStatsPath -> okResponseGenerator(createErroneousBytes),
        MesosAgent.agentContainerStatsPath -> okResponseGenerator(MesosAgentTest.CONTAINER_STATISTICS)
      )
    )

    val timeoutMessage = MesosAgent.produceTimeoutAlertMessage(hostAndPort, MesosAgent.snapshotPath, lowTimeoutConfig)
    val deserializationAlert = MesosAgent.produceDeserializationAlertMessage(MesosAgent.agentExecutorStatsPath, hostAndPort)

    (emitter.emit _).expects(argThat[AlertEvent] { event => event.getDescription == deserializationAlert })
      .once()
    (emitter.emit _).expects(argThat[ServiceMetricEvent] { _: Event => true })
      .repeated(containerCount * metricsPerContainer).times()
    (emitter.emit _).expects(argThat[AlertEvent] { event => event.getDescription == timeoutMessage })
      .once()

    probe()
    closeService.closes should be (1)
    val (_, uris) = environmentHostAndUris(Seq(MesosAgent.agentExecutorStatsPath, MesosAgent.agentContainerStatsPath))
    closeService.assertRequestCount(uris.head, 1)
  }

  def createFreezingEnvironment(path : String, response : () => Response): (Emitter, CountingConstantService, () => Unit, HostAndPort) = {
    createFreezingEnvironment(Map(path -> response))
  }

  def createFreezingEnvironment(map : Map[String, () => Response]): (Emitter, CountingConstantService, () => Unit, HostAndPort) = {
    val hostAndPort: HostAndPort = HostAndPort.fromString("host:5050")

    val map0 = map.map{
      case (path, response) => (MesosAgent.createUri(hostAndPort, path), response)
    }
    createProbeEnvironment(hostAndPort, map0, lowTimeoutConfig)
  }

  private def environmentHostAndUris(uriPath: Seq[String]) = {
    val hostAndPort: HostAndPort = HostAndPort.fromString("host:5050")
    hostAndPort -> uriPath.map(MesosAgent.createUri(hostAndPort, _))
  }

  private def environmentHostAndUris: (HostAndPort, Seq[String]) = {
    environmentHostAndUris(
      Seq(
        MesosAgent.snapshotPath,
        MesosAgent.agentExecutorStatsPath,
        MesosAgent.agentContainerStatsPath
      )
    )
  }


  def createSlaveProbeEnvironment(
    snapshotResponse: () => Response,
    statResponse: () => Response,
    containerStatsResponse: () => Response
  ): (Emitter, CountingConstantService, () => Unit, HostAndPort) = {
    val (hostAndPort, uris) = environmentHostAndUris
    val pathToResponseMap: Map[String, () => Response] = uris
      .zip(Seq(snapshotResponse, statResponse, containerStatsResponse)).toMap
    createProbeEnvironment(hostAndPort, pathToResponseMap)
  }

  def createProbeEnvironment(
    hostAndPort: HostAndPort,
    pathToResponseMap: Map[String, () => Response],
    cfg : HealthMesosConfig = config
  ): (Emitter, CountingConstantService, () => Unit, HostAndPort) =
  {
    val emitter: Emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "sink", emitter)

    val clientService = new CountingConstantService(pathToResponseMap)
    val agent = createAgent(serviceEmitter, hostAndPort, clientService, cfg)

    val probe = () => {
      agent.probeMesosMetrics(Seq(createEndpointDims("", Map.empty, hostAndPort)), MesosAgent.slaveDeserializers)
    }
    (emitter, clientService, probe, hostAndPort)
  }

  "MesosAgent" should "process master snapshot properly" in {
    val emitter = mock[Emitter]
    val other_host = "other_host"
    val other_service = "other_service"
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)

    val metricDimMap = Map[String, Iterable[String]]("dim1" -> Seq("val1"))
    val metricValMap: Map[String, Object] = Map[String, Object]() ++
      mapper.readValue(MesosAgentTest.MASTER_SNAPSHOT, MesosAgent.metricTypeRef)
        .asInstanceOf[Map[String, Object]].filter(
        x => x._2 match {
          case d: java.lang.Double => d != 0.0
          case _ => false
        }
      )
    val fakeHostAndPort = HostAndPort.fromParts(other_host, 5432)

    (emitter.emit _).expects(
      new MockParameter[Event](
        new ArgThat[Event](
          {
            case event: Event =>
              metricValMap.contains(event.toMap.get("metric").toString) &&
                metricDimMap.keys.forall(event.toMap.containsKey) &&
                fakeHostAndPort.toString.equals(event.toMap.get("host")) &&
                other_service.equals(event.toMap.get("service"))
            case _ => false
          }
        )
      )
    ).repeat(58).times()

    val clientService = new CountingConstantService(
      Map(MesosAgent.createUri(fakeHostAndPort, "metrics/snapshot") -> okResponseGenerator(MesosAgentTest.MASTER_SNAPSHOT))
    )

    val agent = createAgent(serviceEmitter, fakeHostAndPort, clientService)

    agent.probeMesosMetrics(
      Seq(createEndpointDims(other_service, metricDimMap, fakeHostAndPort)),
      MesosAgent.masterDeserializers
    )
    clientService.closes should be(1)
    clientService.assertRequestCount(fakeHostAndPort, "metrics/snapshot", 1)
  }

  def createAgent(
    serviceEmitter: ServiceEmitter,
    fakeHostAndPort: HostAndPort,
    clientService: CountingConstantService,
    cfg: HealthMesosConfig = config
  ): MesosAgent = {
    val agent = new MesosAgent(serviceEmitter, None, cfg, rainerKeeper, cfBuilder) {
      override def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
        if (hostAndPort.equals(fakeHostAndPort)) {
          clientService
        } else {
          throw new IllegalStateException("Bad host %s".format(hostAndPort))
        }
      }
    }
    agent
  }

  def createEndpointDims(
    otherService: String,
    metricDimMap: Map[String, Iterable[String]],
    fakeHostAndPort: HostAndPort
  ) : MetricsDimsServiceEndpoint =
  {
    new MetricsDimsServiceEndpoint
    {
      override def metricsDims: Map[String, Iterable[String]] = metricDimMap

      override def hostAndPort: HostAndPort = fakeHostAndPort

      override def service: String = otherService
    }
  }

  "MesosAgent" should "transparently handle parse errors" in {
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val fakeHostAndPort = HostAndPort.fromParts("some_host", 5432)

    val deserializationAlert = MesosAgent.produceDeserializationAlertMessage(MesosAgent.snapshotPath, fakeHostAndPort)
    (emitter.emit _).expects(argThat[AlertEvent] { event => event.getDescription == deserializationAlert }).once

    val uri = MesosAgent.createUri(fakeHostAndPort, MesosAgent.snapshotPath)
    val clientService = new CountingConstantService(Map(uri -> okResponseGenerator(createErroneousBytes)))

    val agent = new MesosAgent(serviceEmitter, None, lowTimeoutConfig, rainerKeeper, cfBuilder) {
      override def fetchClient(hostAndPort: HostAndPort): Service[Request, Response] = {
        if (hostAndPort.equals(fakeHostAndPort)) {
          clientService
        } else {
          throw new IllegalStateException("Bad host %s".format(hostAndPort))
        }
      }
    }

    agent.probeMesosMetrics(
      Seq(createEndpointDims(fakeService, Map.empty, fakeHostAndPort)),
      MesosAgent.masterDeserializers
    )
    clientService.closes should be(1)
    clientService.assertRequestCount(uri, 1)
  }

  // Some tests are ignored. This is so that the testing looks nice and tidy
  // Replace the below `ignore` with `it` to manually test. But then hostnames need changed also
  "Ignored tests" should "not run" in {
    // Except this one
    true should be(true)
  }

  // Sanity check for manual testing
  ignore should "properly probe an actual mesos master" in {
    val master = HostAndPort.fromParts("host.example.com.", 5050)
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    (emitter.emit _).expects(where { event: Event => event.toMap.containsKey("metric") })
      .atLeastOnce()
    agent.probeMesosMetrics(
      Seq(createEndpointDims(fakeService, Map.empty, master)),
      MesosAgent.masterDeserializers
    )
  }

  // Sanity check for manual testing
  ignore should "properly probe an actual mesos master for slaves" in {
    val master = HostAndPort.fromParts("mesos-master.example.com.", 5050)
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    val slaves = agent.getAllSlaves(
      Seq(
        MesosMasterZkInfo(
          master.getHostText,
          "test_master_id",
          master.getPort,
          "test_version"
        )
      )
    )
    slaves should not be 'Empty
  }

  // Sanity check for manual testing
  ignore should "properly probe an actual mesos slave" in {
    val master = HostAndPort.fromParts("host.example.com.", 5051)
    val emitter = mock[Emitter]
    val serviceEmitter = new ServiceEmitter("some_service", "some_host", emitter)
    val agent = new MesosAgent(
      serviceEmitter,
      None,
      config,
      rainerKeeper,
      cfBuilder
    )
    (emitter.emit _).expects(where { event: Event => event.toMap.containsKey("metric") })
      .atLeastOnce()
    agent.probeMesosMetrics(
      Seq(createEndpointDims(fakeService, Map.empty, master)),
      MesosAgent.masterDeserializers
    )
  }

  "MesosAgentTest" should "have proper object fields" in {
    validateIsMap(MesosAgentTest.MASTER_ZK_ENTRY)
    validateIsMap(MesosAgentTest.MASTER_SNAPSHOT)
    validateIsMap(MesosAgentTest.SLAVE_SNAPSHOT)
    validateIsMap(MesosAgentTest.SLAVES)
  }

  "FreezingFuture" should "never return" in {
    val future = new FreezingFuture[AnyRef]
    an[TimeoutException] should be thrownBy Await.result(future, TwitterDuration.fromMilliseconds(100))
  }

  def validateIsMap(bytes: Array[Byte]): Unit = {
    bytes.isEmpty should be(false)
    mapper.readValue(bytes, MesosAgent.metricTypeRef).asInstanceOf[Map[String, Object]].isEmpty should be(false)
  }

  def createInputStream(snapshot: Array[Byte]): ChannelBufferInputStream = {
    new ChannelBufferInputStream(new LittleEndianHeapChannelBuffer(snapshot))
  }

  val createErroneousBytes: Array[Byte] = {
    "{".getBytes("UTF-8")
  }

  private def okResponseGenerator(bytes: Array[Byte]) = {
    () => Response().withEffect(_.content = ByteArray(bytes: _*))
  }

  private def errorResponseGenerator = {
    () => Response(Status.InternalServerError).withEffect(_.contentString = "")
  }

  override def timeLimit: Span = Span(1, Minute)
}

object MesosAgentTest
{
  lazy val MASTER_ZK_ENTRY: Array[Byte] = readBytes("/mesos_master_zk.json")

  lazy val MASTER_SNAPSHOT: Array[Byte] = readBytes("/mesos_master_snapshot.json")

  lazy val SLAVES: Array[Byte] = readBytes("/mesos_slaves.json")

  lazy val SLAVE_SNAPSHOT: Array[Byte] = readBytes("/mesos_slave_snapshot.json")

  lazy val EXECUTOR_STATISTICS: Array[Byte] = readBytes("/mesos_executor_statistics.json")

  lazy val CONTAINER_STATISTICS: Array[Byte] = readBytes("/mesos_container_statistics.json")

  def readBytes(path: String): Array[Byte] = {
    Source.fromURL(getClass.getResource(path)).getLines().mkString.getBytes("UTF-8")
  }
}

// Like twitter's NoFuture but with typing
class FreezingFuture[A](delegate: Option[FreezingFuture[_]] = None) extends Future[A]
{
  @volatile var thread: Option[Thread] = None

  @volatile var raised: Option[Throwable] = None

  private[this] def sleepThenTimeout(timeout: TwitterDuration): TimeoutException = {
    thread = Some(Thread.currentThread())
    try {
      Thread.sleep(timeout.inMilliseconds)
    }
    catch {
      case t: InterruptedException => Thread.interrupted()
    }
    new TimeoutException(s"testing timeout: $timeout")
  }

  override def respond(k: (Try[A]) => Unit): Future[A] = this

  override def transform[B](f: (Try[A]) => Future[B]): Future[B] = new FreezingFuture[B](Some(this))

  override def raise(interrupt: Throwable): Unit = {
    delegate match {
      case Some(d) => d.raise(interrupt)

      case None =>
        raised = Some(interrupt)
        thread.foreach(_.interrupt)
    }
  }

  override def poll: Option[Try[A]] = raised match {
    case Some(t) => Some(
      Try(
        {
          throw t
        }
      )
    )
    case None => None
  }


  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  override def result(timeout: TwitterDuration)(implicit permit: CanAwait): A = {
    val exception = sleepThenTimeout(timeout)
    throw exception
  }

  override def isReady(implicit permit: CanAwait): Boolean = false

  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  override def ready(timeout: TwitterDuration)(implicit permit: CanAwait): this.type =
  {
    val exception = sleepThenTimeout(timeout)
    throw exception
  }
}

class CountingConstantService(responses: Map[String, () => Response]) extends Service[Request, Response] with Matchers
{
  private val closedTimes = new AtomicInteger(0)
  private val requestCounters = mutable.Map[String, AtomicInteger]()

  def apply(request: Request): Future[Response] = {
    requestCounters.getOrElseUpdate(request.uri, new AtomicInteger(0)).incrementAndGet()
    val maybeResponse = responses.get(request.uri)
    if (maybeResponse.isDefined) Future(maybeResponse.get.apply()) else new FreezingFuture[Response]()
  }

  override def close(deadline: Time): Future[Unit] = {
    super.close(deadline) onFailure {
      x => {
        closedTimes.incrementAndGet()
      }
    } onSuccess {
      x => {
        closedTimes.incrementAndGet()
      }
    }
  }

  def closes: Int = closedTimes.get

  def requestCounts: Map[String, Int] = requestCounters.mapValues(_.get()).toMap

  def assertRequestCount(uri: String, expected: Int): Unit = {
    requestCounts.getOrElse(uri, 0) should be(expected)
  }

  def assertRequestCount(hostAndPort: HostAndPort, path: String, expected: Int): Unit = {
    assertRequestCount(MesosAgent.createUri(hostAndPort, path), expected)
  }
}
