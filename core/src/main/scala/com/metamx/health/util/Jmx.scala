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

package com.metamx.health.util

import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef.FinallyOps
import com.twitter.util.Try
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}
import javax.management.{MBeanServerConnection, ObjectName}


object Jmx extends Logging
{
  private val JmxConnectorProperties: java.util.Map[String, _] = {
    import scala.collection.JavaConverters._
    Map(
      "jmx.remote.x.request.waiting.timeout" -> "3000",
      "jmx.remote.x.notification.fetch.timeout" -> "3000",
      "sun.rmi.transport.connectionTimeout" -> "3000",
      "sun.rmi.transport.tcp.handshakeTimeout" -> "3000",
      "sun.rmi.transport.tcp.responseTimeout" -> "3000"
    ).asJava
  }

  def withConnection[T](jmxHost: String, jmxPort: Int)(f: MBeanServerConnection => T): Try[T] = {
    val url = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi".format(jmxHost, jmxPort)
    Try {
      require(jmxPort > 0, "No jmx port but jmx polling enabled!")
      JMXConnectorFactory.connect(new JMXServiceURL(url), JmxConnectorProperties).withFinally(_.close()) { jmxc =>
        f(jmxc.getMBeanServerConnection)
      }
    } onFailure { e =>
      log.error("Failed to connect to %s".format(url), e)
    }
  }

  def getAttribute[T](connection: MBeanServerConnection, objectName: String, attributeName: String): T = {
    connection.getAttribute(new ObjectName(objectName), attributeName).asInstanceOf[T]
  }
}
