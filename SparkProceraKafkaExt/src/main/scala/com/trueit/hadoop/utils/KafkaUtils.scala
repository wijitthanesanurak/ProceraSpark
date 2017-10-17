package com.trueit.hadoop.utils;

import kafka.consumer.SimpleConsumer
import kafka.common.TopicAndPartition
import kafka.common.KafkaException
import kafka.api._
import scala.util._

object KafkaUtils {
  
	def getLastOffset(consumer: SimpleConsumer, topic: String, partition: Int, whichTime: Long): Try[Long] = {
    val tap = new TopicAndPartition(topic, partition)
    val request = new kafka.api.OffsetRequest(Map(tap -> PartitionOffsetRequestInfo(whichTime, 1)))
    val response = consumer.getOffsetsBefore(request)

    if (response.hasError) {
      val err = response.partitionErrorAndOffsets(tap).error
      Failure(new KafkaException("Error fetching data Offset Data the Broker. Reason: " + err))
    } else {
      //offsets is sorted in descending order, we always want the first
      Success(response.partitionErrorAndOffsets(tap).offsets(0))
    }
  }
}
