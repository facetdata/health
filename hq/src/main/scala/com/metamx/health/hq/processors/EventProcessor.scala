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

package com.metamx.health.hq.processors

import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.collection._
import com.metamx.common.scala.concurrent.spawn
import com.metamx.common.scala.control._
import com.metamx.common.scala.event.AlertAggregator
import com.metamx.common.scala.event.Metric
import com.metamx.common.scala.option._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.frdy.codec.OmniDecoder
import com.metamx.metrics.Monitor
import com.metamx.platform.common.kafka.MessageIterators
import java.io.IOException
import java.util.Properties
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import kafka.common.ConsumerRebalanceFailedException
import kafka.message.MessageAndMetadata
import scala.reflect.ClassTag

abstract class EventProcessor[A: ClassTag](
  emitter: ServiceEmitter,
  topic: String,
  kafkaProps: Properties
) extends Logging
{

  private val decoder = OmniDecoder.default[A](DefaultScalaModule, new JodaModule())

  val alertAggregator = new AlertAggregator(log)
  val monitors: Seq[Monitor] = Seq(alertAggregator.monitor)

  // FIXME Nothing cleans up the consumer here
  lazy val iteratorIn = {
    val nextItemFromMultiSource = new ArrayBlockingQueue[MessageAndMetadata[Array[Byte], Array[Byte]]](1)
    val runningCount = new AtomicInteger(0)
    // Handle multi-kafka configuration
    // TODO: It might be a good idea to move multi-kafka support to MessageIterators.
    kafkaProps.getProperty("zookeeper.connect").split(",").foreach {
      zkConnect =>
        val props = new Properties()
        props.putAll(kafkaProps) // can't pass it as `default` in constructor as it won't pass verification (containsKey check) with defaults
        props.setProperty("zookeeper.connect", zkConnect.trim)
        runningCount.incrementAndGet()
        val singleSourceIter = retryOnErrors(
          ifException[IOException] untilPeriod 10.minutes,
          ifException[ConsumerRebalanceFailedException] untilPeriod 10.minutes
        ) {
          MessageIterators.messageIterators(Map(topic -> 1))(props).iterators.onlyElement._2
        }
        spawn {
          while (singleSourceIter.hasNext) {
            nextItemFromMultiSource.put(singleSourceIter.next())
          }
          runningCount.decrementAndGet()
        }
    }

    new Iterator[MessageAndMetadata[Array[Byte], Array[Byte]]]
    {
      override def hasNext: Boolean = runningCount.get() != 0

      override def next(): MessageAndMetadata[Array[Byte], Array[Byte]] = nextItemFromMultiSource.take()
    }
  }

  def run() {
    log.info("Starting processing events from: %s".format(topic))
    while (iteratorIn.hasNext) {
      consume(iteratorIn.next())
    }
  }

  def consume(message: MessageAndMetadata[Array[Byte], Array[Byte]]) {
    val payload = message.message()
    val events = try {
      val frdyResult = decoder.decode(payload)
      frdyResult.objects
    } catch {
      case e: Exception =>
        alertAggregator.put(e, "Failed to decode events from %s".format(topic), Map.empty)
        Nil
    }
    for (event <- events) {
      try {
        process(event)
        metrics(event) ifDefined {
          m =>
            val pfxMetric = Metric(
              metric = m.metric,
              value = m.value,
              userDims = m.userDims.map {
                case (k, v) => ("metric-%s".format(k), v)
              },
              created = m.created
            )
            emitter.emit(pfxMetric)
        }
      } catch {
        case e: Exception =>
          alertAggregator.put(e, "Failed to process event from %s".format(topic), Map("event" -> event))
      }
    }
  }

  def process(event: A)

  def metrics(event: A): Option[Metric] = None
}
