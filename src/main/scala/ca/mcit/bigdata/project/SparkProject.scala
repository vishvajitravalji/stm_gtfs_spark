package ca.mcit.bigdata.project

import java.io.FileNotFoundException
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkProject extends App with Base {

  val stagingPath = new Path("/user/bdss2001/vish1/project5")
  if (fs.exists(stagingPath)) fs.delete(stagingPath, true)
  fs.mkdirs(stagingPath)

  try {
    fs.copyFromLocalFile(new Path("Data\\trips.txt"), new Path("/user/bdss2001/vish1/project5/trips/trips.txt"))
    fs.copyFromLocalFile(new Path("Data\\calendar_dates.txt"), new Path("/user/bdss2001/vish1/project5/calendar_dates/calendar_dates.txt"))
    fs.copyFromLocalFile(new Path("Data\\routes.txt"), new Path("/user/bdss2001/vish1/project5/routes/routes.txt"))
  }
  catch {
    case e: FileNotFoundException => print("File not found==" + e)
  }

  val output = new Path("/user/bdss2001/vish1/project5/enriched_stop_time")
  if (fs.exists(output)) fs.delete(output, true)
  fs.mkdirs(output)

  val tripsDF = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv("/user/bdss2001/vish1/project5/trips/trips.txt")

  val routesDF = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv("/user/bdss2001/vish1/project5/routes/routes.txt")

  val calendarDatesDF = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv("/user/bdss2001/vish1/project5/calendar_dates/calendar_dates.txt")

  tripsDF.createOrReplaceTempView("trips")
  routesDF.createOrReplaceTempView("routes")
  calendarDatesDF.createOrReplaceTempView("calendar_dates")

  val enrichedTrip: DataFrame = spark.sql(
    """SELECT t.route_id, t.service_id, t.trip_id, t.trip_headsign, t.wheelchair_accessible, d.date,d.exception_type, r.route_long_name, r.route_color
      |FROM trips t
      |LEFT JOIN calendar_dates d ON t.service_id = d.service_id
      |LEFT JOIN routes r ON t.route_id = r.route_id""".stripMargin)

  val kafkaConfig: Map[String, String] = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "spark-project",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )

  val topic = "stop_times"

  val kafkaStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig)
  )

  val kafkaStreamValues: DStream[String] = kafkaStream.map(_.value())
  import spark.implicits._
  kafkaStreamValues.foreachRDD(rdd => {
    val stopTimeDf = rdd.map(_.split(",")).map(t => StopTimes(t(0).toInt, t(1), t(2), t(3), t(4).toInt)).toDF
    val enrichedStop = enrichedTrip.join(stopTimeDf, "trip_id")
    enrichedStop.write.mode(SaveMode.Append).csv("/user/bdss2001/vish1/project5/enriched_stop_time")
    enrichedStop.show()
  }
  )
  ssc.start()
  ssc.awaitTermination()
  spark.stop()
}