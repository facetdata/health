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

import com.metamx.common.scala.event._
import com.metamx.common.scala.exception._
import com.metamx.common.scala.{Logging, concurrent}
import com.metamx.emitter.service.AlertEvent
import com.metamx.health.util.AssertiveCloseable
import com.github.nscala_time.time.Imports._
import scala.actors.Actor
import scala.actors.Actor._
import scala.collection._

object MailBuffer
{
  def describe(e: AlertEvent) = {
    subject(e.severity, e.service, e.description)
  }

  def subject(severity: AlertEvent.Severity, service: String, heading: String) = {
    "[%s] %s: %s" format (severity, service, heading)
  }
}

case class MailBufferConfig(
  backoffPeriods: String,
  maxBodySize: Int,
  maxMails: Int,
  mailCutoff: Int
)

// TODO: close Mailer, not MailBuffer

// TODO: there are three different synchronization systems which might lead to unpredictable effects
// 1) Actors (with possible blocking when message queue is too big)
// 2) AssertiveCloseable
// 3) Waiting for closing condition to happen
class MailBuffer(mailer: Mailer, config: MailBufferConfig) extends Logging with AssertiveCloseable
{
  private val backoffStream: Stream[Period] = {
    val seq  = "\\s+".r split config.backoffPeriods map (new Period(_)) // TODO .toStandardDuration
    seq.toStream ++ Stream.continually(seq.last)
  }

  private class BackoffCursor {
    var xs        = backoffStream
    def reset()   { xs = backoffStream }
    def advance() { xs = xs.tail }
    def get       = xs(0)
    def peek      = xs(1)
  }

  private type Subject = String
  private type Body    = String

  def add(subject: Subject, body: Body) {
    assertiveExecute {
      val bodyLimited = if (body.size <= config.maxBodySize) {
        body
      } else {
        "%s ... (%,d more chars)" format (body.take(config.maxBodySize), body.size - config.maxBodySize)
      }
      Q ! Add(subject, bodyLimited)
    }
  }

  private def flush(mail: Mail) {
    val parts = if (mail.count <= config.mailCutoff) {
      mail.bufferTop ++ mail.bufferBot
    } else {
      val omitted = mail.count - config.mailCutoff
      Seq(
        mail.bufferTop,
        Seq("... (%s omitted)" format omitted),
        mail.bufferBot
      ).flatten
    }
    val body = parts mkString "\n\n---\n\n"
    mailer.send(mail.subject, body)
  }

  protected override def assertiveClose() {
    log.info("%s: Closing" format this)
    Q ! Stop()
    stopLock.synchronized {
      while (!stopped) {
        stopLock.wait()
      }
    }
    log.info("%s: Closed" format this)
  }

  private val stopLock = new AnyRef
  private var stopped = false

  private val mails = new mutable.LinkedHashMap[Subject, Mail]
  private class Mail(val subject: String) {
    var count     = 0
    val bufferTop = new mutable.Queue[Body]
    val bufferBot = new mutable.Queue[Body]
    val backoff   = new BackoffCursor

    def clear() {
      count = 0
      bufferTop.clear()
      bufferBot.clear()
      backoff.advance()
    }

    def +=(body: Body) {
      count += 1
      if (bufferTop.size < (config.mailCutoff + 1)/2) {
        bufferTop += body
      } else {
        if (bufferBot.size >= config.mailCutoff/2) {
          log.debug("%s: Discarding body for subject (count[%,d], cutoff[%,d]): %s" format
            (this, count, config.mailCutoff, subject))
          bufferBot.dequeue()
        }
        bufferBot += body
      }
    }
  }

  private val Q: Actor = receiver {

    case Add(subject, body) =>
      if (!(mails contains subject)) {
        for ((subject, mail) <- mails.take(mails.size - config.maxMails + 1)) {
          log.info("%s: Out of capacity, flushing %,d mails from oldest buffer: %s" format
            (this, mail.count, subject))
          flush(mail).catchEither[Throwable] match {
            case Left(e) =>
              // Will retry on next scheduled flush
              log.warn(
                e, "%s: Failed to flush %,d mails, retrying within %s: %s: %s" format
                  (this, mail.count, mail.backoff.get, subject, e)
              )
            case Right(()) =>
              mail.clear()
              mails -= subject
          }
        }
        val mail = new Mail(subject)
        log.debug("%s: Allocating new buffer for %s: %s" format
          (this, mail.backoff.get, subject))
        mails(subject) = mail
        after(mail.backoff.get) { Q ! Flush(subject) }
      }
      val mail = mails(subject)
      log.debug("%s: Buffering mail[buffer.size=%s] until %s: %s" format
        (this, mail.count, mail.backoff.get, subject))
      mail += body

    case Flush(subject) =>
      // Might have been removed due to space constraints in the Add handler.
      mails.get(subject) foreach {
        mail =>
          val mail = mails(subject)
          if (mail.count == 0) {
            log.debug("%s: Removing unused buffer after %s: %s" format
              (this, mail.backoff.get, subject))
            mails -= subject
          } else {
            log.debug("%s: Sending %s mails after %s: %s" format
              (this, mail.count, mail.backoff.get, subject))
            flush(mail).catchEither[Throwable] match {
              case Left(e) =>
                log.warn(
                  e, "%s: Failed to flush %s mails, retrying in %s: %s: %s" format
                    (this, mail.count, mail.backoff.get, subject, e)
                )
              case Right(()) =>
                log.debug("%s: Extending buffer (had %s mails) by %s: %s" format
                  (this, mail.count, mail.backoff.peek, subject))
                mail.clear()
            }
            after(mail.backoff.get) { Q ! Flush(subject) }
          }
      }

    case Stop() =>
      log.info("%s: Stopping and flushing all %s buffers" format(this, mails.size))
      for ((subject, mail) <- mails) {
        log.info("%s: Flushing %,d mails from buffer: %s" format (this, mail.count, subject))
        flush(mail).catchEither[Throwable] match {
          case Left(e) =>
            log.error(
              e, "%s: Failed to flush (and will not retry) %,d mails from buffer: %s"  format
                (this, mail.count, subject)
            )

          case Right(e) =>
            mail.clear()
        }
      }
      mails.clear()
      exit()

      stopLock.synchronized {
        stopped = true
        stopLock.notifyAll()
      }

  }

  private trait Message
  private case class Flush(subject: Subject) extends Message
  private case class Add(subject: Subject, body: Body) extends Message
  private case class Stop() extends Message

  private def after(period: Period)(f: => Unit) { // TODO Duration, not Period
    concurrent.after(period.toStandardDuration.millis) { f }
  }

  private def receiver(f: PartialFunction[Any, Unit]): Actor = {
    actor {
      while (true) {
        receive(f orElse {
          case x =>
            log.error("%s: Actor received unknown message: %s" format (this, x))
        })
      }
    }
  }

  override def toString = "MailBuffer"
}
