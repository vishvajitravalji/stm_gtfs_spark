name := "Project5"

version := "0.1"

scalaVersion := "2.11.8"

val SparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % SparkVersion

