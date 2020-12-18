import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark
import org.apache.spark.sql.Column
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object agg extends App {
  val spark: SparkSession = SparkSession.builder().appName("Ekaterina_Chechik_lab04b").getOrCreate()

    import spark.implicits._

  val schemaJsonValue = StructType(Array(
    StructField ("category", StringType),
    StructField ("event_type", StringType),
    StructField ("item_id", StringType),
    StructField ("item_price", LongType),
    StructField ("timestamp", LongType),
    StructField ("uid", StringType))
  )

  val sdf = spark
    .readStream
    .format("kafka")
    //.trigger(Trigger.ProcessingTime("5 seconds"))
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", "ekaterina_chechik")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "2000")
    .load
    .select('value.cast("string"), 'topic, 'partition, 'offset)
    .repartition(1)
    .select(
      'value.cast("string")
    ).as[String]
    .withColumn("jsonData",from_json(col("value"),schemaJsonValue))
    .select("jsonData.*")
    .withColumn ("timestamp_f", ('timestamp/1000).cast("timestamp"))
    .withColumn ("zeroValue", lit(0))
    .withWatermark("timestamp_f", "1 hour")
    .groupBy(window($"timestamp_f", "1 hour", "1 hour"))
    .agg(
      min('timestamp_f).as("start_ts"),
      sum(when('event_type === "buy", 'item_price).otherwise(lit(0))).as("revenue"),
      sum(when('uid.isNotNull, lit(1)).otherwise(lit(0))).as("visitors"),
      sum(when('event_type === "buy", lit(1)).otherwise(lit(0))).as("purchases")
    )
    .select(
      'start_ts.cast("bigint"),
      ('start_ts.cast("bigint") + 3600).as("end_ts"),
      'revenue,
      'visitors,
      'purchases,
      ('revenue/'purchases).as("aov")
    ).select(to_json(struct(col("*"))).as("value"))

  val datetime_format = DateTimeFormatter.ofPattern("yyyy_MM_dd__HH_mm_ss_SSS")
  val curTime = LocalDateTime.now().format(datetime_format)

  val sink = sdf
    .writeStream
    .format("kafka")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", "ekaterina_chechik_lab04b_out")
    //.option("maxOffsetsPerTrigger", "200")
    //.option("numRows", "20")
    .option("checkpointLocation", s"/user/ekaterina.chechik/chkpnt/$curTime")
    .option("truncate", "false")
    .outputMode("update")

  val sq = sink.start

}
