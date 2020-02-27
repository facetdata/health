package com.metamx.health.mesos

import com.metamx.common.StringUtils
import java.net.URI
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Milliseconds, Span}
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class RainerMesosTest
  extends FlatSpec with Matchers with MockFactory with TimeLimitedTests
{
  val masterPath = "some/master/path"
  val ensemble   = "defaultensemble:2180"
  val baseConfig = StringUtils.toUtf8(
    s"""
       |zkMasterPath: $masterPath
       |zkEnsemble: $ensemble
    """.stripMargin
  )

  // Just use *some* number that is not default
  val zkSessionTimeout    = 998
  val zkConnectionTimeout = 997
  val zkMaxRetries        = 1998
  val zkBaseSleep         = 1997
  val exhibitorURI        = new URI("http://localhost:3818/foo")
  val exhibitorPath       = "/some/new/path"
  val exhibitorMaxRetries = 996
  val exhibitorBaseSleep  = 995
  val exhibitorPollingMs  = 994

  val customConfig = StringUtils.toUtf8(
    s"""
       |zkMasterPath: $masterPath
       |zkEnsemble: $ensemble
       |zkSessionTimeout: $zkSessionTimeout
       |zkConnectionTimeout: $zkConnectionTimeout
       |zkMaxRetries: $zkMaxRetries
       |zkBaseSleep: $zkBaseSleep
       |exhibitorURI: $exhibitorURI
       |exhibitorPath: $exhibitorPath
       |exhibitorMaxRetries: $exhibitorMaxRetries
       |exhibitorBaseSleep: $exhibitorBaseSleep
       |exhibitorPollingMs: $exhibitorPollingMs
    """.stripMargin
  )

  override def timeLimit: Span = Span(10000, Milliseconds)

  "RainerMesos apply" should "properly set defaults" in {
    val rainerMesos = RainerMesos.deserialization.fromKeyAndBytes("", baseConfig)
    rainerMesos.zkMasterPath should be(masterPath)
    rainerMesos.zkEnsemble should be(Some(ensemble))
    rainerMesos.zkSessionTimeout should be(RainerMesos.DEFAULT_ZK_SESSION_TIMEOUT)
    rainerMesos.zkConnectionTimeout should be(RainerMesos.DEFAULT_ZK_CONNECTION_TIMEOUT)
    rainerMesos.exhibitorURI.isEmpty should be(true)
    rainerMesos.exhibitorMaxRetries should be(RainerMesos.DEFAULT_EXHIBITOR_MAX_RETRIES)
    rainerMesos.exhibitorBaseSleep should be(RainerMesos.DEFAULT_EXHIBITOR_BASE_SLEEP)
    rainerMesos.exhibitorPollingMs should be(RainerMesos.DEFAULT_POLLING_MS)
  }

  it should "properly set non-defaults" in {
    val rainerMesos = RainerMesos.deserialization.fromKeyAndBytes("", customConfig)
    rainerMesos.zkMasterPath should be(masterPath)
    rainerMesos.zkEnsemble should be(Some(ensemble))
    rainerMesos.zkSessionTimeout should be(zkSessionTimeout)
    rainerMesos.zkConnectionTimeout should be(zkConnectionTimeout)
    rainerMesos.exhibitorURI.get should equal(exhibitorURI)
    rainerMesos.exhibitorMaxRetries should be(exhibitorMaxRetries)
    rainerMesos.exhibitorBaseSleep should be(exhibitorBaseSleep)
    rainerMesos.exhibitorPollingMs should be(exhibitorPollingMs)
  }
}
