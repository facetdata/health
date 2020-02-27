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

package com.metamx.health.notify

import com.metamx.common.scala.Logging
import java.util.Properties
import javax.mail._
import javax.mail.internet._

case class MailerConfig(
  notifyFormat: String,
  smthHost: String,
  smthPort: Int,
  smtpUser: String,
  smtpPass: String,
  smtpSSLEnabled: Boolean,
  smtpTimeoutMs: Long,
  smtpTo: Seq[String],
  smtpFrom: String,
  smtpDebug: Boolean,
  quiet: Boolean
)

class Mailer(config: MailerConfig) extends Logging
{
  private val props = new Properties
  for ((k,v) <- Map(
    "mail.smtp.debug"             -> config.smtpDebug.toString,
    "mail.smtp.host"              -> config.smthHost,
    "mail.smtp.port"              -> config.smthPort.toString,
    "mail.smtp.connectiontimeout" -> config.smtpTimeoutMs.toString,
    "mail.smtp.timeout"           -> config.smtpTimeoutMs.toString,
    "mail.smtp.ssl.enable"        -> config.smtpSSLEnabled.toString,
    "mail.smtp.auth"              -> "true",
    "mail.smtp.starttls.enable"   -> "true",
    "mail.smtp.ssl.trust"         -> "*"
  )) {
    props.setProperty(k,v)
  }

  private val auth = new Authenticator {
    override def getPasswordAuthentication =
      new PasswordAuthentication(config.smtpUser, config.smtpPass)
  }

  if (config.quiet) {
    log.warn("%s: Starting in a quiet mode" format this)
  }
  else {
    log.info("%s: Starting" format this)
  }

  def send(subjectRaw: String, body: String) {
    val subject = config.notifyFormat.format(subjectRaw)
    log.info("%s: %s -> %s, %s" format (this, config.smtpFrom, config.smtpTo.mkString(","), subject))
    if (config.quiet) {
      log.warn("%s: Withholding mail (quiet mode turned on)" format this)
    }
    else {
      val session = Session.getInstance(props, auth)
      session setDebug config.smtpDebug

      val msg = new MimeMessage(session)
      msg setFrom new InternetAddress(config.smtpFrom)
      for (to <- config.smtpTo) {
        msg addRecipient (Message.RecipientType.TO, new InternetAddress(to))
      }
      msg setSubject subject
      msg setText body

      val transport = session getTransport "smtp"
      transport.connect(config.smthHost, config.smthPort, config.smtpUser, config.smtpPass)
      transport.sendMessage(msg, msg.getAllRecipients)
      transport.close()
    }
  }

  override def toString = "Mailer"
}
