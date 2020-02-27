package com.metamx.platform.common.kafka

import com.metamx.common.lifecycle.LifecycleStop
import com.metamx.common.scala.Logging
import com.metamx.common.scala.exception.ExceptionOps
import java.util.Properties
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.consumer.ConsumerConnector
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

import kafka.message.MessageAndMetadata
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class MessageIterators[+A <: MessageIterators.Iter](
  val consumers: Seq[ConsumerConnector],
  val iterators: Seq[(MessageIterators.Topic, A)]
)
{
  def +[B >: A <: MessageIterators.Iter](x: MessageIterators[B]) = new MessageIterators[B](
    consumers ++ x.consumers,
    iterators ++ x.iterators
  )

  @LifecycleStop
  def stop() {
    consumers.foreach(_.shutdown())
  }
}

class MessageIterator[+A <: MessageIterators.Iter](
  val topic: String,
  val consumerConnector: ConsumerConnector,
  val iterator: A
)

object MessageIterators extends Logging
{
  type Topic = String
  type Iter = Iterator[MessageAndMetadata[Array[Byte], Array[Byte]]]

  def freshConsumerConnector(implicit props: Properties): ConsumerConnector = {
    synchronized {
      Consumer.create(new ConsumerConfig(props))
    }
    // Without the synchronization above, I've repeatedly observed two
    // ConsumerConnectors being constructed simultaneously, with the exact same
    // System.currentTimeMillis timestamp, which produced identical consumerIds
    // and threw a ZK error. Forcing a sync ensures the timestamps and
    // thus consumerIds are different.
  }

  def messageIterators(topics: Map[Topic, Int])(implicit props: Properties): MessageIterators[Iter] = {
    val consumerConnector = freshConsumerConnector

    try {
      new MessageIterators(
        Seq(consumerConnector),
        for {
          (topic, kafkaStreams) <- consumerConnector.createMessageStreams(topics).toSeq
          kafkaStream           <- kafkaStreams
        } yield (topic, kafkaStream.iterator())
      )
    } catch {
      case e: Exception =>
        consumerConnector.shutdown()
        throw e
    }
  }

  def manualCommitMessageIterators(
    topics: Map[Topic, Int]
  )(
    implicit props: Properties
  ): Seq[MessageIterator[Iter with CommitOffsets]] = {

    // We must set auto.commit.enable = false, or else the consumer will commit offsets on its own.
    val manualCommitProps = {
      val p = new Properties
      props.foreach { case (k,v) => p(k) = v }
      p("auto.commit.enable") = "false"
      p
    }

    // To manually commit offsets you must use ConsumerConnector.commitOffsets, which commits
    // offsets for all topic-partition pairs owned by that consumer (connector). The common case is
    // for each user thread to (1) own a message iterator, (2) not own a consumer, and (3) commit in
    // isolation, so we force the user to create a new consumer for each topic-partition pair, and
    // we bundle the commit function with the message iterator. The downside of this is more
    // coordination, which scales linearly (or more?) with the number of consumers.

    val iterators = ArrayBuffer[MessageIterator[Iter with CommitOffsets]]()

    for (
      (topic, n) <- topics.toSeq;
      _          <- 0 until n
    ) {
      val consumerConnector = freshConsumerConnector(manualCommitProps)

      try {
        val kafkaStream = consumerConnector.createMessageStreams(Map(topic -> 1)).toSeq match {
          case Seq((t, List(s))) if t == topic => s
        }

        iterators += new MessageIterator(
          topic,
          consumerConnector,
          new Iter with CommitOffsets
          {
            private val iter = kafkaStream.iterator()

            def hasNext: Boolean = iter.hasNext()

            def next(): MessageAndMetadata[Array[Byte], Array[Byte]] = iter.next()

            def commitOffsets() {
              consumerConnector.commitOffsets(retryOnFailure = true)
            }
          }
        )
      } catch {
        case e: Exception =>
          consumerConnector.shutdown().swallow {
            case e: Exception => log.error(e, "Unable to shutdown consumer connector")
          }

          iterators.foreach {
            s => s.consumerConnector.shutdown().swallow {
              case e: Exception => log.error(e, "Unable to shutdown consumer connector")
            }
          }

          throw e
      }
    }

    iterators
  }

}

trait CommitOffsets {
  def commitOffsets()
}
