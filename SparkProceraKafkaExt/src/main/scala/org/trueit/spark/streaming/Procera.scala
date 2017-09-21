package org.trueit.spark.streaming

import org.apache.spark.SparkConf
//import org.apache.spark.streaming
//import org.apache.spark.streaming.kafka
import com.trueit.crypto._
import com.ippontech.kafka.KafkaSource
import com.ippontech.kafka.stores.ZooKeeperOffsetsStore
import kafka.serializer.StringDecoder
import org.apache.spark.{Accumulator, AccumulatorParam, SparkContext}
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import com.synergistic.it.util._
//import org.apache.spark.streaming._
import java.util.Date
import java.text.SimpleDateFormat 
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.functions._

case class Procera (
  probe_id					: Long,		//1    int
  destinationipv4address	: String,	//2    string
  destinationtransportport	: Long,		//3    int
  egressinterface			: Long,		//4    int 
  flowendseconds			: Long,		//5    timestamp
  flowstartseconds			: Long,		//6    timestamp
  ingressinterface			: Long,		//7    int
  octettotalcount			: Long,		//8    bigint      
  packettotalcount			: Long,		//9    bigint   
  proceraapn				: String,	//10   string
  proceracontentcategorie	: String,	//11   string
  proceradeviceid			: Long,		//12   bigint
  proceraexternalrtt		: Long,		//13   int
  proceraggsn				: String,	//14   string
  procerahttpcontenttype	: String,	//15   string
  procerahttpfilelength		: Long,		//16   int
  procerahttplanguage		: String,	//17   string
  procerahttplocation		: String,	//18   string
  procerahttppreferer		: String,	//19   string
  procerahttprequestmethod	: String,	//20   string
  proceraresponsestatus		: Long,		//21   int 
  procerahttpurl			: String,	//22   string
  procerahttpuseragent		: String,	//23   string
  proceraimsi				: Long,		//24   bingint
  proceraincomingoctets		: Long,		//25   bigint
  proceraincomingpackets	: Long,		//26   bigint
  procerainternalrtt		: Long,		//27   int
  proceramsisdn				: Long,		//28   bigint
  proceraoutgoingoctets		: Long,		//29   bigint
  proceraoutgoingpackets	: Long,		//30   bigint
  proceraqoeincomingexternal: Float,	//31   float
  proceraqoeincominginternal: Float,	//32   float
  proceraqoeoutgoingexternal: Float,	//33   float
  proceraqoeoutgoinginternal: Float,	//34   float
  procerarat				: String,	//35   string
  proceraserverhostname		: String,	//36   string
  proceraservice			: String,	//37   string
  procerasgsn				: String,	//38   string
  procerauserlocationinformation:String,//39   string
  protocolidentifier		: Long,		//40   int
  sourceipv4address			: String,	//41   string
  sourcetransportport		: Long,		//42   int
  //// extended field
  bu						: String,	//43   string
  sitename					: String,	//44   string
  filename					: String,   //45   string
  ld_date					: String	//46   string
)

object ProceraCompute
{
	val date_add = udf(() => {
    	val sdf = new SimpleDateFormat("yyyy-MM-dd")
    	val result = new Date()
  		sdf.format(result)
	})

	val encrypt: String => String = new DESedeEncryption().encrypt(_)
	val encryptUDF = udf(encrypt)
	
	val decrypt: String => String = new DESedeEncryption().decrypt(_)
	val decryptUDF = udf(decrypt)
	
	def main(args: Array[String])
	{
		if(args.length < 3) {
			System.err.println("\nUsage: ProceraCompute <kafka_topic> <interval> <anonymize,y|n>")
			System.err.println("Sample: ProceraCompute test-in 5 y")
			System.exit(1)
		}

		val zkGroupTopicDirs = new kafka.utils.ZKGroupTopicDirs("ProceraKafkaEtl", "test");

		val zkHost = "hw-nn-02.dc1.true.th"
		val zkPath = zkGroupTopicDirs.consumerOffsetDir 
System.err.println("zkPath:" + zkPath)
		val Array(topic_s, interval, is_anonymous) = args

		System.out.println("Kafka Topic  : " + topic_s)
		System.out.println("Interval Time: " + interval)
		System.out.println("Anonymous    :" + is_anonymous)

		val conf = new SparkConf().setAppName("ProceraKafkaEtl")

		val sc = new SparkContext(conf)
		
		val ssc = new org.apache.spark.streaming.StreamingContext(sc, org.apache.spark.streaming.Seconds(interval.toInt))
		
		val kafkaParams = Map[String, String] (
			//"metadata.broker.list" -> "10.128.0.3:9092",
			//"metadata.broker.list" -> "cjkafdc01:9092,cjkafdc02:9092,cjkafdc03:9092",
			"metadata.broker.list" -> "hw-ax-01.dc1.true.th:9092,hw-ax-02.dc1.true.th:9092",
			"group.id" -> "ProceraKafkaEtl"
			//"auto.offset.reset" -> "largest"
		)

		try {
			//val messages = KafkaSource.kafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, new //ZooKeeperOffsetsStore(zkHost, zkPath), topic_s)	
			val topics = List(topic_s).toSet
			val messages = org.apache.spark.streaming.kafka.KafkaUtils.createDirectStream[String, String, StringDecoder,
							StringDecoder](ssc, kafkaParams, topics)
							
			val sqlContext = new SQLContext(sc)
			/*
			messages.foreachRDD(rdd =>
				rdd.collect.foreach(record=>
					println(record)
				)
			)
			*/
			
			val csv = messages.map(_._2).map(c=>c.replaceAll("[\\r\\n]", "\0")).map(rdd=>rdd.split(','))
			/*
			csv.foreachRDD( rdd => {
							rdd.collect.foreach( record => {
								  println(record(0) + "," + record(1) + "," + record(2) + ":" + record)
								})    
							})
			*/
			//csv.foreachRDD(record=>println(record.count))
			
			import sqlContext.implicits._
			csv.foreachRDD { rdd =>
				import sqlContext.implicits._
				val pcr = rdd.map(m=>Procera(
					m(0).toLong, 
					m(1), 
					m(2).toLong, m(3).toLong, m(4).toLong, m(5).toLong, m(6).toLong, 
					m(7).toLong, m(8).toLong, 
					m(9), m(1), 
					m(11).toLong,
					m(12).toLong,	// =>Int, 
					m(13), m(14), 
					m(15).toLong, 
					m(16), m(17), m(18), m(19),
					m(20).toLong, 
					m(21), m(22), 
					m(23).toLong, 
					m(24).toLong, 
					m(25).toLong,	// =>Int, 
					m(26).toLong,
					m(27).toLong, 
					m(28).toLong, 
					m(29).toLong,
					m(30).toFloat, 
					m(31).toFloat, 
					m(32).toFloat, 
					m(33).toFloat, 
					m(34), m(35),
					m(36), m(37), m(38), m(39).toLong, m(40), 
					m(41).toLong,
					m(42), m(43), "", m(44)
				))

				val df = sqlContext.createDataFrame(pcr).toDF()
				df.show()
				

				//val df_valid = df.withColumn("proceraexternalrtt", when(col("proceraexternalrtt")===4294967295, 0))
				
				val df_2 = df.withColumn("flowendseconds", $"flowendseconds".cast("timestamp"))
				//val df_1 = df.withColumn("flowendseconds", $"flowendseconds".cast("timestamp"))
							 .withColumn("flowstartseconds", $"flowstartseconds".cast("timestamp"))
							 .withColumn("probe_id", $"probe_id".cast("int"))
							 .withColumn("destinationtransportport", $"destinationtransportport".cast("int"))
							 .withColumn("egressinterface", $"egressinterface".cast("int"))
							 .withColumn("ingressinterface", $"ingressinterface".cast("int"))
							 .withColumn("proceraexternalrtt", $"proceraexternalrtt".cast("int"))
							 .withColumn("procerahttpfilelength", $"procerahttpfilelength".cast("int"))
							 .withColumn("proceraresponsestatus", $"proceraresponsestatus".cast("int"))
							 .withColumn("procerainternalrtt", $"procerainternalrtt".cast("int"))
							 .withColumn("protocolidentifier", $"protocolidentifier".cast("int"))
							 .withColumn("sourcetransportport", $"sourcetransportport".cast("int"))		 

				//val df_2 = df_1.withColumn("ld_date", date_add())

				//if(df_2.count > 0) {
				if(df_2.count > 0) {
					if(is_anonymous == "y" || is_anonymous == "Y")
					{
						val encryptedDF = df_2.withColumn("encrypted", encryptUDF('proceramsisdn))
						val encryptedNoMsisDnDF = encryptedDF.drop("proceramsisdn")	
						val encryptedMvDF = encryptedNoMsisDnDF.withColumnRenamed("encrypted",
														"proceramsisdn")
						encryptedMvDF.printSchema()
						encryptedMvDF.show()
						encryptedMvDF.groupBy("ld_date").count.show
						encryptedMvDF.write.format("parquet").mode(org.apache.spark.sql.SaveMode.Append).
								partitionBy("ld_date").
								saveAsTable("procera_a") 
					}
					else {
						df_2.write.format("parquet").mode(org.apache.spark.sql.SaveMode.Append).
								partitionBy("ld_date").
								saveAsTable("procera") 
						df_2.printSchema()
						df_2.show()
						df_2.groupBy("ld_date").count.show
					}
				}
				
			} 
		}
		catch {
			case nfe: NumberFormatException => None 
			case ae:  java.lang.ArithmeticException => 0
			case rt:  RuntimeException => 0
			case oob: java.lang.ArrayIndexOutOfBoundsException => println("Error...")
			case nfe: java.lang.NumberFormatException => 0
			//case oor: OffsetOutOfRangeError => 
			case _ => Iterator("An unexpected error has occurred. We are so sorry!")		
		}
		
		ssc.start()
		ssc.awaitTermination()
	}
}

