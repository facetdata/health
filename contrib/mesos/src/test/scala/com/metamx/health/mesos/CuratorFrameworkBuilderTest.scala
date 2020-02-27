package com.metamx.health.mesos

import java.net.URI
import org.apache.curator.ensemble.exhibitor.{ExhibitorEnsembleProvider, ExhibitorRestClient}
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider
import org.apache.curator.framework.CuratorFrameworkFactory
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Milliseconds, Span}
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class CuratorFrameworkBuilderTest extends FlatSpec with Matchers with MockFactory with TimeLimitedTests
{
  val rainerMesos   = RainerMesos(
    zkMasterPath = "/master/path",
    zkEnsemble = Some("127.0.0.1:9999"),
    zkSessionTimeout = 150,
    zkConnectionTimeout = 190,
    zkMaxRetries = 8,
    zkBaseSleep = 178,
    exhibitorURI = Option(new URI("http://localhost:9988/foo")),
    exhibitorMaxRetries = 2,
    exhibitorBaseSleep = 110,
    exhibitorPollingMs = 10001
  )
  val curatorClient = new ExhibitorRestClient() {
    override def getRaw(
      hostname: String,
      port: Int,
      uriPath: String,
      mimeType: String
    ): String = {
      if (new URI(s"http://$hostname:$port/$uriPath") == rainerMesos.exhibitorURI.get) {
        rainerMesos.zkEnsemble.get
      } else {
        throw new IllegalArgumentException
      }
    }
  }

  override def timeLimit: Span = Span(1000L, Milliseconds)

  "CuratorFrameworkBuilder" should "build with basic config" in {
    val cfBuilder = CuratorFrameworkFactory.builder()
    val curatorFrameworkBuilder = new CuratorFrameworkBuilder(
      "default:1245",
      builder = cfBuilder,
      client = curatorClient
    )
    val _ = curatorFrameworkBuilder(rainerMesos)
    cfBuilder.getEnsembleProvider.getClass should equal(classOf[ExhibitorEnsembleProvider])
  }

  it should "build with old config" in {
    // A bunch of defaults
    val oldRainerMesos = RainerMesos(
      zkMasterPath = rainerMesos.zkMasterPath,
      zkEnsemble = rainerMesos.zkEnsemble,
      zkSessionTimeout = rainerMesos.zkSessionTimeout,
      zkConnectionTimeout = rainerMesos.zkConnectionTimeout,
      zkMaxRetries = rainerMesos.zkMaxRetries,
      zkBaseSleep = rainerMesos.zkBaseSleep,
      exhibitorURI = None, // This one changes the config method
      exhibitorMaxRetries = rainerMesos.exhibitorMaxRetries,
      exhibitorBaseSleep = rainerMesos.exhibitorBaseSleep,
      exhibitorPollingMs = rainerMesos.exhibitorPollingMs
    )
    // Allow override of builder
    val cfBuilder = CuratorFrameworkFactory.builder()
    val curatorFrameworkBuilder = new CuratorFrameworkBuilder(
      "default:1234",
      builder = cfBuilder,
      client = curatorClient
    )
    val _ = curatorFrameworkBuilder(oldRainerMesos)
    cfBuilder.getEnsembleProvider.getClass should equal(classOf[FixedEnsembleProvider])
  }
}
