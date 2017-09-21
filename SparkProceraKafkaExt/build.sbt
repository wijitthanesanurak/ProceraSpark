name := "SparkPoceraKafkaExt"
version := "1.1"
scalaVersion := "2.11.8"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided" 
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"





assemblyMergeStrategy in assembly := {  
case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
