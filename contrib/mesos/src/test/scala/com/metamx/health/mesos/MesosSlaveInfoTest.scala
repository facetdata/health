package com.metamx.health.mesos

import com.google.common.net.HostAndPort
import org.scalatest.{FlatSpec, Matchers}

class MesosSlaveInfoTest
  extends FlatSpec with Matchers
{
  val host        = "192.168.1.1"
  val port        = 2134
  val id          = "some_id"
  val version     = "some_version"
  val hostAndPort = HostAndPort.fromParts(host, port)

  "MesosSlaveInfo" should "return valid values" in {
    val info = MesosSlaveInfo(id, s"some_pid@$hostAndPort", version)
    info.hostAndPort should equal(hostAndPort)
    info.metricsDims should equal(
      Map(
        "mesos/host" -> Seq(host),
        "mesos/port" -> Seq(port.toString),
        "mesos/id" -> Seq(id),
        "mesos/version" -> Seq(version)
      )
    )
  }
}
