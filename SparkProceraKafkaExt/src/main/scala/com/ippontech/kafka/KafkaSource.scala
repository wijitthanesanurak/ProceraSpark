package com.ippontech.kafka

import com.ippontech.kafka.stores.OffsetsStore
//import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag

//object KafkaSource extends LazyLogging {
object KafkaSource {

	def kafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag] (ssc: StreamingContext, kafkaParams: Map[String, String], offsetsStore: OffsetsStore, topic: String): InputDStream[(K, V)] = {

		//val topics = Set(topic)
		val topics = List(topic).toSet

System.err.println("Before readOffset")
		val storedOffsets = offsetsStore.readOffsets(topic)
		val kafkaStream = storedOffsets match {
		case None =>
System.err.println("None: start from the latest offsets")
			// start from the latest offsets
System.err.println(kafkaParams)
System.err.println(topics)
System.err.println(ssc)
			KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topics)
		case Some(fromOffsets) =>
System.err.println("In case Some")
			// start from previously saved offsets
			val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
System.err.println("Before createDirectStream")
			KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, fromOffsets, messageHandler)
	}

	// save the offsets
System.err.println("Before save offset")
	kafkaStream.foreachRDD(rdd => offsetsStore.saveOffsets(topic, rdd))

	kafkaStream
}

// Kafka input stream
def kafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag] (ssc: StreamingContext, brokers: String, offsetsStore: OffsetsStore, topic: String): InputDStream[(K, V)] =
	kafkaStream(ssc, Map("metadata.broker.list" -> brokers), offsetsStore, topic)
}
