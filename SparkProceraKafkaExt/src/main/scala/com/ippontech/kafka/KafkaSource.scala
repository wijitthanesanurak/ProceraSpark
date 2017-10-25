package com.ippontech.kafka

import com.ippontech.kafka.stores.OffsetsStore
//import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

//object KafkaSource extends LazyLogginfg {
object KafkaSource {
  val log = LogManager.getRootLogger
  
	def kafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag] 
    (ssc: StreamingContext, kafkaParams: Map[String, String], offsetsStore: OffsetsStore, topic: String): 
    InputDStream[(K, V)] = {

		//val topics = Set(topic)
		val topics = List(topic).toSet

		val storedOffsets = offsetsStore.readOffsets(topic)

		val kafkaStream = storedOffsets match {
		case None =>
			// start from the latest offsets
		  log.info("In case createDirectStream")
			KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topics)
		case Some(fromOffsets) =>
			// start from previously saved offsets
		  log.info("In case Some offsets:" + fromOffsets)
			val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)

			KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, fromOffsets, messageHandler)
		}
	  // save the offsets
	  kafkaStream.foreachRDD(rdd => offsetsStore.saveOffsets(topic, rdd))
	
	  kafkaStream
  }

  // Kafka input stream
  def kafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag] 
  (ssc: StreamingContext, brokers: String, offsetsStore: OffsetsStore, topic: String): 
  InputDStream[(K, V)] =
  	kafkaStream(ssc, Map("metadata.broker.list" -> brokers), offsetsStore, topic)
	
  def kafkaStreamWriteOnly[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag] 
      (ssc: StreamingContext, kafkaParams: Map[String, String], offsetsStore: OffsetsStore, topic: String): 
      InputDStream[(K, V)] = {
  
  		//val topics = Set(topic)
  		val topics = List(topic).toSet
  
  		val storedOffsets = offsetsStore.readOffsets(topic)
  
  		log.info("kafkaStreamWriteOnly")
  		
  		val kafkaStream = KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topics)
      
  		// save the offsets
  		kafkaStream.foreachRDD(rdd => offsetsStore.saveOffsets(topic, rdd))
  	
      kafkaStream
  }	
}


