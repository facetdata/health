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

import org.joda.time.DateTime
import scala.xml.{Node, NodeSeq, XML}

class GmailRssFeed(url: String)
{
  def getAllEmails(): Seq[Email] =
  {
    val root = XML.loadString(url)
    (root \\ "entry").map(buildEmails).toIndexedSeq
  }

  def buildEmails(node: Node): Email =
  {
    new Email(
      this,
      (node \\ "title").text,
      (node \\ "summary").text,
      buildAuthor(node \\ "author"),
      DateTime.parse((node \\ "modified").text),
      DateTime.parse((node \\ "issued").text)
    )
  }

  def buildAuthor(node: NodeSeq): Author =
  {
    new Author(
      (node \\ "name").text,
      (node \\ "email").text
    )
  }

  case class Author(
    name: String,
    emailId: String
  )

  case class Email(
    parent: GmailRssFeed,
    title: String,
    summary: String,
    author: Author,
    modified: DateTime,
    issued: DateTime
  )
}
