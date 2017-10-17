package com.ippontech.kafka.stores

import com.ippontech.kafka.utils.Stopwatch
//import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.common.TopicAndPartition
import kafka.utils.{ ZKStringSerializer, ZkUtils }
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

//class ZooKeeperOffsetsStore(zkHosts: String, zkPath: String) extends OffsetsStore with LazyLogging {
class ZooKeeperOffsetsStore(zkHosts: String, zkPath: String) extends OffsetsStore {

  private val zkClient = new ZkClient(zkHosts, 10000, 10000, ZKStringSerializer)
  val log = LogManager.getRootLogger

  // Read the previously saved offsets from Zookeeper
  override def readOffsets(topic: String): Option[Map[TopicAndPartition, Long]] = {

    log.info("Reading offsets from Zookeeper")

    val stopwatch = new Stopwatch()

      val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)

      log.info("offsetsRangersStrOpt match")

      offsetsRangesStrOpt match {
        case Some(offsetsRangesStr) =>

          log.info("In case offsetsRangerStr" + offsetsRangesStr)

          //logger.debug(s"Read offset ranges: ${offsetsRangesStr}")
          //System.err.println(offsetsRangerStr)
          val offsets = offsetsRangesStr.split(",")
            .map(s => s.split(":"))
            .map {
              case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong)
            }
            .toMap

          log.info("Done reading offsets from ZooKeeper. Took " + stopwatch)
          log.info("Offset stamp: " + offsetsRangesStr)

          Some(offsets)
        case None =>
          log.info("No offsets found in ZooKeeper. Took " + stopwatch)
          None
      }
  }

  // Save the offsets back to ZooKeeper
  //
  // IMPORTANT: We're not saving the offset immediately but instead save the offset from the previous batch. This is
  // because the extraction of the offsets has to be done at the beginning of the stream processing, before the real
  // logic is applied. Instead, we want to save the offsets once we have successfully processed a batch, hence the
  // workaround.
  override def saveOffsets(topic: String, rdd: RDD[_]): Unit = {

    //logger.info("Saving offsets to ZooKeeper")
    val stopwatch = new Stopwatch()

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //offsetsRanges.foreach(offsetRange => 
    //	logger.debug(s"Using ${offsetRange}"))

    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}").mkString(",")
    //logger.debug(s"Writing offsets to ZooKeeper: ${offsetsRangesStr}")
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)

    log.info("Done updating offsets in ZooKeeper. Took " + stopwatch)
  }
}
