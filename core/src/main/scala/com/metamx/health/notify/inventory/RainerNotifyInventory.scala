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

package com.metamx.health.notify.inventory

import com.google.common.base.Throwables
import com.metamx.common.scala.concurrent.locks.LockOps
import com.metamx.common.scala.event.emit
import com.metamx.common.scala.untyped._
import com.metamx.common.scala.{Logging, Yaml}
import com.metamx.emitter.service.AlertEvent.Severity
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.health.core.config.ServiceConfig
import com.metamx.health.notify._
import com.metamx.health.rainer.RainerUtils
import com.metamx.health.rainer.RainerUtils.ConfigData
import com.metamx.rainer.{Commit, CommitKeeper, KeyValueDeserialization}
import com.twitter.util.Witness
import java.io.ByteArrayInputStream
import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

// TODO: If this has not initialized within 5 mins kill
class RainerNotifyInventory(
  emitter: ServiceEmitter,
  rainerConfigKey: String,
  rainerKeeper: CommitKeeper[NotifyInventoryConfig],
  serviceConfig: ServiceConfig
) extends NotifyInventory with Logging
{
  class RainerNotifyInventoryState(
    val config: NotifyInventoryConfig,
    val inventory: StaticNotifyInventory
  )

  // read lock - for config users, write lock - for config updates
  private val lock = new ReentrantReadWriteLock()

  private val initialized = new AtomicBoolean(false)
  @volatile private var stateOpt: Option[RainerNotifyInventoryState] = None

  private def withState[A](f: (RainerNotifyInventoryState) => A): A = {
    initialized.synchronized {
      if (!initialized.get()) {
        log.info("Waiting for backing inventory initialization")

        while (!initialized.get()) {
          try {
            initialized.wait()
          } catch {
            case e: InterruptedException => // NOP
          }
        }
      }
    }

    val state = stateOpt getOrElse {
      throw new IllegalStateException("Backing inventory not initialized")
    }
    f(state)
  }

  private lazy val closer = rainerKeeper.mirror().changes.register(
    new Witness[Map[Commit.Key, Commit[NotifyInventoryConfig]]]
    {
      override def notify(note: Map[Commit.Key, Commit[NotifyInventoryConfig]]) {
        lock.writeLock() {
          log.info("Reinitializing backing inventory – received configs update notification")
          try {
            val configData = RainerUtils.configByKey(rainerConfigKey, note)

            configData match {
              case Some(ConfigData(newConfig, newConfigStr)) =>
                log.info("Inventory config for key '%s':\n%s\n\n".format(rainerConfigKey, newConfigStr))

                try {
                  stateOpt match {
                    case Some(state) if state.config == newConfig =>
                      log.info("Doing nothing - new and old configs are identical")

                    case _ =>
                      stateOpt = Some(new RainerNotifyInventoryState(newConfig, new StaticNotifyInventory(newConfig, serviceConfig)))
                  }
                } finally {
                  initialized.synchronized {
                    initialized.set(true)
                    initialized.notifyAll()
                  }
                }

              case None =>
                val description = "Unable to reinitialize backing inventory - missing config"
                val details = Map(
                  "rainerConfigKey" -> rainerConfigKey
                )
                emit.emitAlert(log, emitter, Severity.SERVICE_FAILURE, description, details)
            }
          } catch {
            case e: Throwable =>
              val description = "Unable to reinitialize backing inventory - exception"
              val details = Map(
                "exceptionType" -> e.getClass.getName,
                "exceptionMessage" -> e.getMessage,
                "exceptionStackTrace" -> Throwables.getStackTraceAsString(e)
              )
              emit.emitAlert(e, log, emitter, Severity.SERVICE_FAILURE, description, details)
          }
          log.info("Backing inventory reinitialized")
        }
      }
    }
  )

  def init() {
    // Force initialization
    closer

    // Waiting for the first rainer config update
    withState { _ => }
  }

  override def withMailer[A](f: (Mailer) => A): A = {
    lock.readLock() {
      withState { state => state.inventory.withMailer(f) }
    }
  }

  override def withMailBuffer[A](f: (MailBuffer) => A): A = {
    lock.readLock() {
      withState { state => state.inventory.withMailBuffer(f) }
    }
  }

  override def withPagerDuty[A](f: (PagerDuty) => A): A = {
    lock.readLock() {
      withState { state => state.inventory.withPagerDuty(f) }
    }
  }
}

object RainerNotifyInventory extends Logging
{
  class InnerDict(umbrellaDict: Dict, innerKey: String)
  {
    val d: Dict = umbrellaDict.get(innerKey).map(dict(_)).getOrElse(Map.empty)

    def parse[A](key: String, f: Option[Any] => A): A = {
      val value = d.get(key)
      try {
        f(value)
      }
      catch {
        case e: Exception =>
          throw new IllegalArgumentException("Unable to parse '%s'.'%s' from value '%s'".format(innerKey, key, value), e)
      }
    }
  }

  implicit val deserialization = new KeyValueDeserialization[NotifyInventoryConfig] {
    override def fromKeyAndBytes(k: Commit.Key, bytes: Array[Byte]) = {
      val d = dict(Yaml.load(new ByteArrayInputStream(bytes)))
      val configDict = dict(d("config"))

      val mailerDict = new InnerDict(configDict, "mailer")
      val mailBufferDict = new InnerDict(configDict, "mailBuffer")
      val pagerDutyDict = new InnerDict(configDict, "pagerDuty")
      NotifyInventoryConfig(
        mailerConfig = MailerConfig(
          notifyFormat = mailerDict.parse("notifyFormat", x => x.map(str(_)).getOrElse("%s")),
          smthHost = mailerDict.parse("smtpHost", x => str(x.get)),
          smthPort = mailerDict.parse("smtpPort", x => int(x.get)),
          smtpUser = mailerDict.parse("smtpUser", x => str(x.get)),
          smtpPass = mailerDict.parse("smtpPass", x => str(x.get)),
          smtpSSLEnabled = mailerDict.parse("smtpSSLEnabled", x => x.forall(bool(_))),
          smtpTimeoutMs = mailerDict.parse("smtpTimeoutMs", x => long(x.get)),
          smtpTo = mailerDict.parse("smtpTo", x => str(x.get).split(',')),
          smtpFrom = mailerDict.parse("smtpFrom", x => str(x.get)),
          smtpDebug = mailerDict.parse("smtpDebug", x => x.exists(bool(_))),
          quiet = mailerDict.parse("quiet", x => x.exists(bool(_)))
        ),
        mailBufferConfig = MailBufferConfig(
          backoffPeriods = mailBufferDict.parse("backoffPeriods", x => str(x.get)),
          maxBodySize = mailBufferDict.parse("maxBodySize", x => x.map(int(_)).getOrElse(100000)),
          maxMails = mailBufferDict.parse("maxMails", x => x.map(int(_)).getOrElse(100)),
          mailCutoff = mailBufferDict.parse("mailCutoff", x => x.map(int(_)).getOrElse(10))
        ),
        pagerDutyConfig = PagerDutyConfig(
          postUrl = pagerDutyDict.parse("postUrl", x => new URI(str(x.get))),
          quiet = pagerDutyDict.parse("quiet", x => x.exists(bool(_))),
          mailOnly = pagerDutyDict.parse("mailOnly", x => x.exists(bool(_)))
        )
      )
    }
  }
}
