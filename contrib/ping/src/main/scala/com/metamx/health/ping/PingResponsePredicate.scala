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

package com.metamx.health.ping

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.untyped._
import com.metamx.health.ping.PingResponsePredicate.PredicateFn
import java.util.regex.Pattern
import javax.script.Invocable
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

/**
 * Simple trait to help out the JS scripting engine. It has trouble implementing Function1 directly.
 */
trait ResponsePredicate
{
  def apply(response: PingResponse): Any
}

object PingResponsePredicate
{
  type PredicateFn = PingResponse => PingResult

  private val DefaultPredicate: PredicateFn = response => PingResult(response.statusCode == 200)

  def default = DefaultPredicate

  def fromString(s: String): PredicateFn = JsPredicate(s)

  def fromDict(d: Dict): PredicateFn = d.get("type") match {
    case Some("gmailRss") =>
      val senderAddress = str(d("senderAddress"))
      val threshold = new Period(str(d.getOrElse("threshold", "PT2H")))
      val titleRegex = Pattern.compile(str(d.getOrElse("titleRegex", ".*")))
      GmailRssPredicate(titleRegex, senderAddress, threshold)

    case Some("js") =>
      JsPredicate(str(d.getOrElse("function", throw new IllegalArgumentException("Missing 'function'"))))

    case Some(x) =>
      throw new IllegalArgumentException("Unknown predicate type")

    case None =>
      throw new IllegalArgumentException("Missing predicate type")
  }

  def fromAny(o: Any): PredicateFn = tryCasts(o)(
    x => fromString(str(x)),
    x => fromDict(dict(x))
  )
}

object JsPredicate
{
  def apply(function: String): PredicateFn =
  {
    val engine = JsScriptEngine.create()
    engine.eval("var apply = %s" format function)

    val predicate = engine.asInstanceOf[Invocable].getInterface(classOf[ResponsePredicate])
    response => tryCasts(predicate(response))(
      res => PingResult(res.asInstanceOf[Boolean]),
      res => res.asInstanceOf[PingResult]
    )
  }
}

object GmailRssPredicate extends Logging
{
  def apply(
    titleRegex: Pattern,
    senderAddress: String,
    threshold: Period
  ): PredicateFn =
  {
    response => {
      if (response.statusCode == 200) {
        val feed = new GmailRssFeed(response.body)
        val happyHour = DateTime.now(DateTimeZone.UTC).minus(threshold)
        val allMails = feed.getAllEmails()
        val matchingMails = allMails.filter(
          mail =>
            mail.author.emailId == senderAddress.trim &&
              titleRegex.matcher(mail.title).matches()
        )
        val recentMails = matchingMails.filter(
          email =>
            email.issued.isAfter(happyHour)
        )
        log.debug(
          "Found %,d total mails, %,d mails matching predicate, %,d mails matching threshold.",
          allMails.size,
          matchingMails.size,
          recentMails.size
        )
        PingResult(
          recentMails.size > 0,
          Dict(
            "total mails" -> allMails.size,
            "matching mails" -> matchingMails.size,
            "recent mails" -> recentMails.size,
            "newest matching mail" ->
              matchingMails.sortBy(_.issued.getMillis).lastOption.map(_.issued.toString()).getOrElse("none")
          )
        )
      } else {
        PingResult(false)
      }
    }
  }
}
