package com.trueit.spark.streaming

import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka
import org.apache.spark.rdd.RDD
import com.ippontech.kafka.KafkaSource
import com.ippontech.kafka.stores.ZooKeeperOffsetsStore
import kafka.serializer.StringDecoder
import org.apache.spark.{Accumulator, AccumulatorParam, SparkContext}
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import java.util.Date
import java.text.SimpleDateFormat 
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import com.trueit.crypto._
import com.trueit.crypto.aes._
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.UUID

case class Procera (
  probe_id                  : Long,		//1    int
  destinationipv4address    : String,	//2    string
  destinationtransportport  : Long,		//3    int
  egressinterface           : Long,		//4    int 
  flowendseconds            : Long,		//5    timestamp
  flowstartseconds          : Long,		//6    timestamp
  ingressinterface          : Long,		//7    int
  octettotalcount           : Long,		//8    bigint      
  packettotalcount          : Long,		//9    bigint   
  proceraapn				        : String,	//10   string
  proceracontentcategorie	  : String,	//11   string
  proceradeviceid			      : Long,		//12   bigint
  proceraexternalrtt		    : Long,		//13   int
  proceraggsn				        : String,	//14   string
  procerahttpcontenttype	  : String,	//15   string
  procerahttpfilelength		  : Long,		//16   int
  procerahttplanguage		    : String,	//17   string
  procerahttplocation		    : String,	//18   string
  procerahttppreferer		    : String,	//19   string
  procerahttprequestmethod	: String,	//20   string
  proceraresponsestatus		  : Long,		//21   int 
  procerahttpurl			      : String,	//22   string
  procerahttpuseragent		  : String,	//23   string
  proceraimsi				        : Long,		//24   bingint
  proceraincomingoctets		  : Long,		//25   bigint
  proceraincomingpackets	  : Long,		//26   bigint
  procerainternalrtt		    : Long,		//27   int
  proceramsisdn				      : Long,		//28   bigint
  proceraoutgoingoctets		  : Long,		//29   bigint
  proceraoutgoingpackets	  : Long,		//30   bigint
  proceraqoeincomingexternal: Float,	//31   float
  proceraqoeincominginternal: Float,	//32   float
  proceraqoeoutgoingexternal: Float,	//33   float
  proceraqoeoutgoinginternal: Float,	//34   float
  procerarat				        : String,	//35   string
  proceraserverhostname		  : String,	//36   string
  proceraservice			      : String,	//37   string
  procerasgsn				        : String,	//38   string
  procerauserlocationinformation:String,//39   string
  protocolidentifier		    : Long,		//40   int
  sourceipv4address			    : String,	//41   string
  sourcetransportport		    : Long,		//42   int
  //// extended field
  bu						            : String,	//43   string
  sitename					        : String,	//44   string
  filename					        : String, //45   string
  ld_date					          : String	//46   string
)

object SparkProceraComputeExt {
  val log = LogManager.getRootLogger
  val key = "1234567890123456"
  
  val date_add = udf(() => {
    	val sdf = new SimpleDateFormat("yyyy-MM-dd")
    	val result = new Date()
  		sdf.format(result)
	})

	//val encrypt: String => String = new DESedeEncryption().encrypt(_)
	val encrypt = (input:String) => {
	  AesEcbPkcs5Padding.encrypt(input, key)
	}
	val encryptUDF = udf(encrypt)
	
	//val decrypt: String => String = new DESedeEncryption().decrypt(_)
	val decrypt = (input:String) => {
	  AesEcbPkcs5Padding.decrypt(input, key)
	}
	val decryptUDF = udf(decrypt)

	def toLong(s: String): Long = {
    try {
      s.toLong
    } catch {
      case nfe: NumberFormatException => 0
    }
  }
	
	def toFloat(s: String): Float = {
    try {
      s.toFloat
    } catch {
      case nfe: NumberFormatException => (0.0).toFloat
    }
  }
	  
  def doAnonymize(sqlContext:SQLContext, rdd: RDD[Array[String]], 
                  is_anonymize:String, time1:org.apache.spark.streaming.Time) = {
System.out.println("Start doAnonymize")
//System.out.println("rdd count: " + rdd.count())
    //log.info("Start doAnonymize")
    //log.info("rdd count: " + rdd.count())
    try {
      sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
      import  sqlContext.implicits._
			val pcr = rdd.map(m=>Procera(
				//m(0).toLong, 
				toLong(m(0)),
			  m(1), 
				toLong(m(2)), toLong(m(3)), toLong(m(4)), toLong(m(5)), toLong(m(6)), 
				toLong(m(7)), toLong(m(8)), 
				m(9), m(1), 
				toLong(m(11)),
        toLong(m(12)),	// =>Int, 
				m(13), m(14), 
				toLong(m(15)), 
				m(16), m(17), m(18), m(19),
				toLong(m(20)), 
				m(21), m(22), 
				toLong(m(23)), 
				toLong(m(24)), 
				toLong(m(25)),	// =>Int, 
				toLong(m(26)),
				toLong(m(27)), 
				toLong(m(28)), 
				toLong(m(29)),
        toFloat(m(30)), 
				toFloat(m(31)), 
				toFloat(m(32)), 
				toFloat(m(33)), 
				m(34), m(35),
        m(36), m(37), m(38), 
        toLong(m(39)), m(40), 
				toLong(m(41)),
				m(42), m(43), "", m(44)
			))
	
			val df = sqlContext.createDataFrame(pcr).toDF()
			//df.show()
			
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
						 
			if(df_2.count > 0) {
				if(is_anonymize == "y" || is_anonymize == "Y")
				{
					val encryptedDF = df_2.withColumn("encrypted", encryptUDF('proceramsisdn))
					val encryptedNoMsisDnDF = encryptedDF.drop("proceramsisdn")	
					val encryptedMvDF = encryptedNoMsisDnDF.withColumnRenamed("encrypted",
													"proceramsisdn")
					//encryptedMvDF.printSchema()
//					encryptedMvDF.show()
//					encryptedMvDF.groupBy("ld_date").count.show
//					System.out.println("Before write")
					val randomStr = UUID.randomUUID().toString()
					//encryptedMvDF.write.format("parquet").mode(org.apache.spark.sql.SaveMode.Append).
							//partitionBy("ld_date").
							//save("/user/tapadm/wijit/procera.db/tbl_mobile/" + randomStr)
//				  	  saveAsTable("procera_a.tbl_mobile")
          val ld_date = encryptedMvDF.first().getString(44)
					//encryptedMvDF.write.parquet("/user/tapadm/wijit/procera.db/tbl_mobile1/ld_date=" + 
					//                    ld_date + "/"+time.toString()+"/")
					encryptedMvDF.write.parquet("/user/tapadm/wijit/procera.db/tbl_mobile1/ld_date=" + 
					                    ld_date + "/" + time1.toString() + "/")								
//					System.out.println("End write")
System.out.println("End Doannymize")													
				}
				else {
					//df_2.printSchema()
					//df_2.show()
					//df_2.groupBy("ld_date").count.show
					log.info("Before write")
				  df_2.write.format("parquet").mode(org.apache.spark.sql.SaveMode.Append).
							partitionBy("ld_date").
							saveAsTable("procera_a.tbl_mobile")
					log.info("End write")
				}
				
				log.info("Finish DoAnonymize")
			}
    } catch {
      case oob: java.lang.ArrayIndexOutOfBoundsException =>
        //oob.printStackTrace()
        log.error("ArrayIndexOutOfBoundException")
      case jie: java.io.IOException =>
        jie.printStackTrace()
    }
  }
  
  def run(kafkaParams: Map[String, String], topic_s: String, interval: Int,
          is_anonymize: String) {
    val conf = new SparkConf().setAppName("SparkProcera")
    val sc = new SparkContext(conf)
    val ssc = new org.apache.spark.streaming.StreamingContext(sc, org.apache.spark.streaming.Seconds(interval.toInt))

    val sqlContext = new SQLContext(sc)
    
    //val zkGroupTopicDirs = new kafka.utils.ZKGroupTopicDirs("ProceraKafkaEtl", "test");
    //val zkGroupTopicDirs = new kafka.utils.ZKGroupTopicDirs("ProceraKafkaEtl", topic_s);
    val zkGroupTopicDirs = new kafka.utils.ZKGroupTopicDirs("ProceraKafkaEtl", topic_s);

    val zkHost = "mtg8-tmp-01.tap.true.th:2181,mtg8-tmp-02.tap.true.th:2181,mtg8-tmp-03.tap.true.th:2181/kafka"
		//val zkHost = "localhost"
    //val zkHost = "10.128.0.3"
    //val zkHost = "172.16.2.110" 
		val zkPath = zkGroupTopicDirs.consumerOffsetDir 
		//val zkPath = "/kafka/kafka" + _zkPath
		log.info("Zookeeper Path: " + zkPath)
    
		val messages = KafkaSource.kafkaStream[String, String, StringDecoder, 
		  StringDecoder](ssc, kafkaParams, new ZooKeeperOffsetsStore(zkHost, zkPath), topic_s)	
		
		  
		  
//		val topics = List(topic_s).toSet
//    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val csv = messages.map(_._2).map(c => c.replaceAll("[\\r\\n]", "\0")).map(rdd => rdd.split(','))
//		log.info("csv : " + csv.count())
    val csv_45 = csv.filter(m => m.size == 45)
  
//    csv_45.foreachRDD(rdd=>rdd.count())
    //log.info("csv_45: " + csv_45.count())
    
//    csv_45.foreachRDD((rdd,time) => doAnonymize(sqlContext, rdd, is_anonymize, time))
    csv_45.foreachRDD((rdd,time) => if (rdd.count()>0) doAnonymize(sqlContext, rdd, is_anonymize, time) else log.warn(s"Batch $time has no transaction"))
    //csv_45.foreachRDD(rdd=>rdd.foreach(println))
		ssc.start()
    ssc.awaitTermination()
  }
    
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("\nUsage : DashboardCompute <topic-source> <topic-target> <interval> <anonymous,y/n>")
      System.err.println("Sample: DashboardCompute test-3 test55 5 y\n")
      System.exit(1)
    }

    val Array(topic_s, topic_d, interval, is_anonymize) = args

    // List of topics you want to listen for from Kafka
    val topics = List(topic_s).toSet

    val kafkaParams = Map[String, String](
      //"metadata.broker.list" -> "10.128.0.3:9092",
      //"metadata.broker.list" -> "172.16.2.110:9092,172.16.2.111:9092,172.16.2.112:9092",
      //"metadata.broker.list" -> "localhost:9092",
      //"metadata.broker.list" -> "cjkafdc01:9092,cjkafdc02:9092,cjkafdc03:9092",
      //"metadata.broker.list" -> "hw-ax-01.dc1.true.th:9092,hw-ax-02.dc1.true.th:9092",
      "metadata.broker.list" -> "mtg8-tmp-01.tap.true.th:9092,mtg8-tmp-02.tap.true.th:9092,mtg8-tmp-03.tap.true.th:9092",
      "group.id" -> "ProceraKafkaEtl",
      "auto.offset.reset" -> "largest"
      //"security.protocol" -> "SASL_PLAINTEXT"
    )

    log.info("Topic Source :" + topic_s)
    log.info("Topic Dest   :" + topic_d)
    log.info("Duration Time:" + interval + " sec.")
    log.info("Anonymous    :" + is_anonymize)

    run(kafkaParams, topic_s, interval.toInt, is_anonymize)
    
    log.error("Spark ended")

  }
}
