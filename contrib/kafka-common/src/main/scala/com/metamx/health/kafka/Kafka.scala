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

package com.metamx.health.kafka

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.scala.Logger
import com.metamx.common.scala.Predef.EffectOps
import com.metamx.common.scala.exception.ExceptionOps
import com.metamx.common.scala.untyped
import com.metamx.common.scala.untyped.Dict
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.FuturePool
import java.util.concurrent.Executors
import kafka.api._
import kafka.cluster.BrokerEndPoint
import kafka.common.ErrorMapping
import kafka.common.KafkaException
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import scala.annotation.tailrec
import scala.util.Random
import scala.util.Try

class Kafka(executorsPoolSize: Int, log: Logger)
{
  private val exec = FuturePool(
    Executors.newFixedThreadPool(
      executorsPoolSize,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build()
    )
  )

  /**
   * For each consumer offset in consumerOffsets which is the offset of a last processed message  fetches messages
   * and also fetches the last message stored in broker. It's expected that messages are stored in frdy format
   * and have "received" or "t" header which contains the time when the message was created.
   * If the message by the given offset doesn't exist on the kafka broker, the first message
   * will be fetched if given offset is less than the lowest offset on broker and or the last
   * message will be fetched if given offset is greater than the largest offset on the broker.
   * Delta is calculated as (last broker message creation time – last consumed message creation time).
   */
  def measureBacklog(
    consumerOffsets: Seq[TopicPartitionOffset],
    brokers: Seq[BrokerEndPoint],
    props: KafkaProperties
  ): Try[Map[TopicPartitionOffset, DeltaWithAlerts]] =
  {
    Try {
      val topics = consumerOffsets.groupBy(_.topic).keys.toSeq
      val leadersByTP = fetchPartitionsMetadata(topics, brokers, props).flatMap {
        case (topic, Some(partitions)) =>
          partitions.map {
            partitionMetadata =>
              val leader = partitionMetadata.leader.getOrElse {
                throw new IllegalStateException("Leader is not available for %s-%s".format(topic, partitionMetadata.partitionId))
              }
              TopicAndPartition(topic, partitionMetadata.partitionId) -> leader
          }
        case (topic, None) => throw new KafkaMetadataException("Couldn't fetch metadata for %s".format(topic))
      }

      val tposByLeader = consumerOffsets.groupBy {
        tpo => leadersByTP.getOrElse(
          TopicAndPartition(tpo.topic, tpo.partition),
          throw new KafkaMetadataException(
            "Couldn't fetch metadata for %s".format(TopicAndPartition(tpo.topic, tpo.partition))
          )
        )
      }

      val deltaFuturesSeq = tposByLeader.map {
        case (leader, tpos) =>
          val brokerOffsetsFuture = exec {
            withConsumer(leader, props.soTimeout, props.bufferSize) { consumer =>
              val topicAndPartitions = tpos.map(tpo => TopicAndPartition(tpo.topic, tpo.partition))
              // With kafka API we can't make a single offset request for the same topic-partition
              val earliestOffsets = fetchOffsets(topicAndPartitions, consumer, OffsetRequest.EarliestTime)
              val latestOffsets = fetchOffsets(topicAndPartitions, consumer, OffsetRequest.LatestTime)
              topicAndPartitions.map { tp => tp -> (earliestOffsets(tp), latestOffsets(tp)) }.toMap
            }
          }

          brokerOffsetsFuture.flatMap {
            case brokerOffsets =>
              Future.collect(
                tpos.map {
                  tpo =>
                    val topic = tpo.topic
                    val partition = tpo.partition
                    val (earliestOffset, latestOffset) = brokerOffsets(TopicAndPartition(topic, partition))

                    // Using next offset is better than last committed consumerOffset as consumer is actually
                    // up to any time before next message as there were no new messages.
                    // Also, it solves problem of having enormous deltas when there were no new messages for a long time
                    // and at the moment of check we got new message but have not committed checkpoint yet.
                    // Example:
                    // now is 02:00:10, time for current offset is 00:00:00.
                    // Time for next consumer offset is 02:00:00 (no messages from 00:00 to 02:00).
                    // Last time consumer committed offset is 01:59:50, so it is only 20 seconds from the moment
                    // when consumer was up to date. But if we calculate delta from current consumer it will be 2h.
                    // To get the correct result we need to calculate delta from the next offset to the latest message.
                    val nextConsumerOffset = tpo.offset + 1
                    val alertDetails = Map(
                      "topic" -> topic,
                      "partition" -> partition,
                      "nextConsumerOffset" -> nextConsumerOffset,
                      "brokerEarliestOffset" -> earliestOffset,
                      "brokerLatestOffset" -> latestOffset
                    )

                    val alerts = nextConsumerOffset match {
                      case o if o < earliestOffset =>
                        log.error("Lost data: next consumer offset %s for %s-%s is less than earliest offset %s".format(
                          nextConsumerOffset, topic, partition, earliestOffset)
                        )
                        Seq(Alert("Lost data for topic %s".format(topic), alertDetails))
                      case o if o > latestOffset =>
                        log.error(
                          "Consumer ahead: next consumer offset %s for %s-%s is more than latest offset (next on broker) %s".format(
                            nextConsumerOffset, topic, partition, latestOffset
                          )
                        )
                        Seq(Alert("Consumer ahead broker for %s".format(topic), alertDetails))
                      case o => Seq()
                    }

                    exec {
                      withConsumer(leader, props.soTimeout, props.bufferSize){
                        consumer =>
                          // With kafka API we can't make a single request with more than one fetch (different offsets)
                          // for the same topic-partition. We still can make it in parallel with different threads,
                          // but it shouldn't be significant improvement compared to parallel requests for different
                          // topic-partitions and reuse of consumer for the same topic-partition. If we still
                          // want to optimize message fetching we can try making batched requests for different
                          // topic-partitions located on the same leader.
                          val brokerAdjustedOffset = adjustOffset(latestOffset, earliestOffset, latestOffset)
                          val brokerTime = fetchMessageAndExtractTime(topic, partition, props, consumer, brokerAdjustedOffset)
                          val consumerAdjustedOffset = adjustOffset(nextConsumerOffset, earliestOffset, latestOffset)
                          val consumerTime = fetchMessageAndExtractTime(topic, partition, props, consumer, consumerAdjustedOffset)

                          val delta = PartitionDelta(
                            math.max(brokerAdjustedOffset - consumerAdjustedOffset, 0L),
                            math.max(brokerTime - consumerTime, 0L)
                          )
                          tpo -> DeltaWithAlerts(delta, alerts)
                      }
                    }
                }
              )
          }
      }.toSeq

      Await.result(Future.collect(deltaFuturesSeq)).flatten.toMap
    }
  }

  def fetchPartitionsMetadata(
    topics: Seq[String],
    brokers: Seq[BrokerEndPoint],
    props: KafkaProperties
  ): Map[String, Option[Seq[PartitionMetadata]]] =
  {
    @tailrec def fetchPartitionsMetadata(
      remainedBrokers: Seq[BrokerEndPoint]
    ): Map[String, Option[Seq[PartitionMetadata]]] =
    {
      remainedBrokers.headOption match {
        case None =>
          throw new KafkaMetadataException(
            "Exhausted brokers while fetching metadata for topics %s from %s".format(topics, brokers.mkString(", "))
          )

        case Some(broker) =>
          try {
            withConsumer(broker, props.soTimeout, props.bufferSize) {
              consumer =>
                val response = consumer.send(new TopicMetadataRequest(topics, 0))
                response.topicsMetadata.map {
                  topicMetadata =>
                    if (topicMetadata.errorCode == ErrorMapping.NoError) {
                      for (partitionMetadata <- topicMetadata.partitionsMetadata) {
                        val errorCode = partitionMetadata.errorCode
                        if (errorCode != ErrorMapping.NoError && errorCode != ErrorMapping.ReplicaNotAvailableCode) {
                          throw new KafkaMetadataException(
                            "Error while processing metadata for topic %s, partition %s from %s".format(
                              topicMetadata.topic, partitionMetadata.partitionId, broker
                            )
                          )
                        }
                      }

                      topicMetadata.topic -> Some(topicMetadata.partitionsMetadata)
                    } else if (topicMetadata.errorCode == ErrorMapping.UnknownTopicOrPartitionCode) {
                      topicMetadata.topic -> None
                    } else {
                      throw new KafkaMetadataException(
                        "Error while fetching metadata for topic %s from %s".format(topicMetadata.topic, broker),
                        ErrorMapping.exceptionFor(topicMetadata.errorCode)
                      )
                    }
                }.toMap
            }
          } catch {
            case e: KafkaMetadataException => throw e
            case e: Throwable =>
              log.warn(e, "Recoverable error while fetching metadata for topics %s from %s".format(topics, broker))
              fetchPartitionsMetadata(remainedBrokers.tail)
          }
      }
    }
    fetchPartitionsMetadata(Random.shuffle(brokers))
  }

  def fetchPartitionsMetadata(
    topic: String,
    brokers: Seq[BrokerEndPoint],
    props: KafkaProperties
  ): Option[Seq[PartitionMetadata]] =
  {
    fetchPartitionsMetadata(Seq(topic), brokers, props)(topic)
  }

  def fetchMessage(
    topic: String,
    partition: Int,
    props: KafkaProperties,
    leader: BrokerEndPoint,
    offset: Long
  ): Option[MessageAndOffset] =
  {
    withConsumer[Option[MessageAndOffset]](leader, props.soTimeout, props.bufferSize) {
      consumer => fetchMessage(topic, partition, props, consumer, offset)
    }
  }

  def fetchMessage(
    topic: String,
    partition: Int,
    props: KafkaProperties,
    consumer: SimpleConsumer,
    offset: Long
  ): Option[MessageAndOffset] =
  {
    val messages = fetchMessages(topic, partition, props, consumer, offset)
    messages.find(_.offset == offset)
  }

  def fetchMessages(
    topic: String,
    partition: Int,
    props: KafkaProperties,
    leader: BrokerEndPoint,
    startOffset: Long
  ): Iterable[MessageAndOffset] =
  {
    withConsumer(leader, props.soTimeout, props.bufferSize) {
      consumer => fetchMessages(topic, partition, props, consumer, startOffset)
    }
  }

  def fetchMessages(
    topic: String,
    partition: Int,
    props: KafkaProperties,
    consumer: SimpleConsumer,
    startOffset: Long
  ): Iterable[MessageAndOffset] =
  {
    val request = new FetchRequestBuilder()
      .addFetch(topic, partition, startOffset, props.fetchSize)
      .maxWait(props.maxWait)
      .minBytes(1)
      .build()

    val response = consumer.fetch(request)
    if (response.hasError) {
      log.warn(
        "Failed to fetch messages for %s-%s from %s:%s starting with offset %s. Error code: %s".format(
          topic, partition, consumer.host, consumer.port, startOffset, response.errorCode(topic, partition)
        )
      )
      throw ErrorMapping.exceptionFor(response.errorCode(topic, partition))
    }
    response.messageSet(topic, partition)
  }

  def fetchOffset(
    topic: String,
    partition: Int,
    props: KafkaProperties,
    leader: BrokerEndPoint,
    time: Long
  ): Long =
  {
    val tp = TopicAndPartition(topic, partition)
    fetchOffsets(Seq(tp), props, leader, time)(tp)
  }

  def fetchOffsets(
    topicAndPartitions: Seq[TopicAndPartition],
    props: KafkaProperties,
    leader: BrokerEndPoint,
    time: Long
  ): Map[TopicAndPartition, Long] =
  {
    withConsumer(leader, props.soTimeout, props.bufferSize) {
      consumer =>
        fetchOffsets(topicAndPartitions, consumer, time)
    }
  }

  def fetchOffsets(
    topicAndPartitions: Seq[TopicAndPartition],
    consumer: SimpleConsumer,
    time: Long
  ): Map[TopicAndPartition, Long] = {
    val requestOffset = OffsetRequest(
      requestInfo = topicAndPartitions.map { tp => tp -> PartitionOffsetRequestInfo(time, 1) }.toMap
    )

    val offsetResponse = consumer.getOffsetsBefore(requestOffset)
    offsetResponse.partitionErrorAndOffsets.map {
      case (tp, response) =>
        response.error match {
          case ErrorMapping.NoError => tp -> response.offsets.head
          case _ => throw new KafkaException(
            "Error while fetching offset for %s-%s".format(tp.topic, tp.partition),
            ErrorMapping.exceptionFor(response.error)
          )
        }
    }
  }

  def fetchMessageAndExtractTime(
    topic: String,
    partition: Int,
    props: KafkaProperties,
    leader: BrokerEndPoint,
    offset: Long
  ): Long =
  {
    withConsumer(leader, props.soTimeout, props.bufferSize) {
      consumer => fetchMessageAndExtractTime(topic, partition, props, consumer, offset)
    }
  }

  def fetchMessageAndExtractTime(
    topic: String,
    partition: Int,
    props: KafkaProperties,
    consumer: SimpleConsumer,
    offset: Long
  ): Long =
  {
    val offsetMessageOpt = fetchMessage(topic, partition, props, consumer, offset)

    offsetMessageOpt match {
      case Some(messageAndOffset) =>
        val payload = new Array[Byte](messageAndOffset.message.payload.limit())
          .withEffect(messageAndOffset.message.payload.get(_))

        val header = FrdyHeaderOmniDecoder.decoder.decodeHeader(payload)
        val timestamp = header.getOrElse(
          "received", header.getOrElse(
            "t",
            throw new IllegalStateException(
              """Unable to get time for message @ %s, %s-%s: missing "received" or "t" header""".format(
                offset, topic, partition
              )
            )
          )
        )
        untyped.long(timestamp)

      case None => 0
    }
  }

  private def adjustOffset(offset: Long, min: Long, max: Long): Long = {
    require(min <= max, "Max offset should be more than min offset or equal. max=%,d, min=%,d".format(max, min))

    // We try to return max-1 because if we fetch data by max offset it will be empty.
    math.max(math.min(max - 1, offset), min)
  }

  private def withConsumer[A](
    broker: BrokerEndPoint,
    soTimeout: Int,
    bufferSize: Int,
    clientId: String = ConsumerConfig.DefaultClientId
  )(f: (SimpleConsumer) => A): A =
  {
    val consumer = new SimpleConsumer(broker.host, broker.port, soTimeout, bufferSize, clientId)
    try {
      f(consumer)
    }
    finally {
      consumer.close() swallow { case e: Exception => }
    }
  }
}

case class KafkaProperties(
  soTimeout: Int,
  bufferSize: Int,
  fetchSize: Int,
  maxWait: Int
)

case class TopicPartitionOffset(topic: String, partition: Int, offset: Long)

case class PartitionDelta(messages: Long, time: Long) {
  override def toString = "message delta: %,d, time delta: %,d ms".format(messages, time)

  def +(partitionDelta: PartitionDelta): PartitionDelta = {
    PartitionDelta(this.messages + partitionDelta.messages, this.time + partitionDelta.time)
  }
}

case class Alert(description: String, data: Dict)

case class DeltaWithAlerts(delta: PartitionDelta, alerts: Seq[Alert])

class KafkaMetadataException(message: String, throwable: Throwable) extends RuntimeException(message, throwable)
{
  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)
}
