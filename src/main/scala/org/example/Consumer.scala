package org.example

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object Consumer {

  def getConsumer(properties: Properties): KafkaConsumer[String, String] = {
    new KafkaConsumer(properties, new StringDeserializer, new StringDeserializer)
  }

  def pollRecords(
      consumer: KafkaConsumer[String, String],
      partitions: List[TopicPartition],
      partition: TopicPartition,
      maxMessagesCount: Int
  ): ConsumerRecords[String, String] = {
    val partitionsOffsetsMap = consumer.endOffsets(partitions.asJava)
    consumer.assign(List(partition).asJava)
    consumer.seek(partition, partitionsOffsetsMap.get(partition) - maxMessagesCount)
    consumer.poll(Duration.ofMillis(1500))
  }

  def print(
      records: ConsumerRecords[String, String]
  ): Unit = {
    records
      .forEach(r =>
        println(
          s"Partition:${r.partition} Offset:${r.offset} ${r.value}"
        )
      )
  }

  def main(args: Array[String]): Unit = {
    val maxMessagesCount = 5
    val topic            = "books"

    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "consumer1")

    val consumer = getConsumer(properties)

    val partition0 = new TopicPartition(topic, 0)
    val partition1 = new TopicPartition(topic, 1)
    val partition2 = new TopicPartition(topic, 2)

    val partitions = List(partition0, partition1, partition2)

    val recordsPartition0 = pollRecords(consumer, partitions, partition0, maxMessagesCount)
    val recordsPartition1 = pollRecords(consumer, partitions, partition1, maxMessagesCount)
    val recordsPartition2 = pollRecords(consumer, partitions, partition2, maxMessagesCount)

    consumer.close()

    print(recordsPartition0)
    print(recordsPartition1)
    print(recordsPartition2)
  }
}
