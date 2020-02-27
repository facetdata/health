package com.metamx.health.mesos

import com.google.common.net.HostAndPort
import org.scalatest.{FlatSpec, Matchers}

class MesosMasterZkInfoTest
  extends FlatSpec with Matchers
{
  val id       = "some_id"
  val version  = "some_version"
  val hostname = "192.168.1.1"
  val port     = 2134
  "MesosMasterZkInfo" should "return good values" in {
    val info = new MesosMasterZkInfo(
      hostname,
      id,
      port,
      version
    )
    info.hostAndPort should equal(HostAndPort.fromParts(hostname, port))
    info.metricsDims should equal(
      Map[String, Iterable[String]](
        "mesos/version" -> Seq(version),
        "mesos/hostname" -> Seq(hostname),
        "mesos/id" -> Seq(id),
        "mesos/port" -> Seq(port.toString)
      )
    )
  }
}
