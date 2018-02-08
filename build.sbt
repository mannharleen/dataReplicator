name := "dataReplicator"
version := "0.1"

//scalaVersion := "2.12.4"
scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.0.0"
//libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.2.1"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.1"
//libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.apache.hadoop" % "hadoop-common" % "2.7.1",
  //"org.apache.hadoop" % "hadoop-aws" % "2.7.4",
  "org.apache.parquet" % "parquet-avro" % "1.8.1",
  "org.apache.avro"  %  "avro"  %  "1.7.7",
  "com.univocity"  %  "univocity-parsers"  %  "2.5.9",
  "org.json"  %  "json" % "20171018"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.concat
  case _ => MergeStrategy.first
}
