name := "SparkProceraKafkaExt"
version := "1.1"
scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"
//libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.7.2"
//libraryDependencies += "com.typesafe.scala-logging" % "scala-logging-slf4j_2.11" % "2.1.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.3"
//libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.11.0.0"
//libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.11.0.0"
//libraryDependencies += "org.I0Itec" % "zkclient" % "0.10"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

assemblyMergeStrategy in assembly := {  
case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
