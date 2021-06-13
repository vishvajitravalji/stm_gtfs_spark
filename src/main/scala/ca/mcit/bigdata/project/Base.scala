package ca.mcit.bigdata.project

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait Base {

  //Hadoop Conf
  val conf = new Configuration()
  val hadoopConfDir = "C:\\Users\\Vish\\Desktop\\hadoop"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))
  val fs = FileSystem.get(conf)

  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Project")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))

}
