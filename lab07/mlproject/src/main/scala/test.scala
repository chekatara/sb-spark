import common.ConfigReader.conf
import org.apache.spark.sql._
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

import scala.concurrent.duration.DurationInt
import scala.util.Random

object test extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("ekaterina_chechik_lab07")
    .config("spark.executor.instances", "10")
    .config("spark.executor.cores", "8")
    .config("spark.executor.memory", "4g")
    .config("spark.default.parallelism", 100)
    .config("spark.sql.shuffle.partitions",100)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._

  val hdfsModelPath: String = spark.conf.get("spark.mlproject.model_dir", conf.hdfsModelPath)
  val kafkaTestHosts = spark.conf.get("spark.mlproject.test.kafka.hosts", conf.kafkaTestHosts)
  val kafkaTestStartingOffsets = spark.conf.get("spark.mlproject.test.kafka.starting_offsets",
    conf.kafkaTestStartingOffsets)
  val kafkaTestMaxOffsetsPerTrigger = spark.conf.get("spark.mlproject.test.kafka.max_offsets",
    conf.kafkaTestMaxOffsetsPerTrigger)
  val kafkaTestInputTopic: String = spark.conf.get("spark.mlproject.test.kafka.input_topic",
    conf.kafkaTestInputTopic)
  val kafkaTestOutputTopic: String = spark.conf.get("spark.mlproject.test.kafka.output_topic",
    conf.kafkaTestOutputTopic)

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_hh_mm_ss")
  val dateTimeNow: String = LocalDateTime.now.format(formatter)
  val kafkaCheckPointLocation = spark.conf.get("spark.agg.kafka.checkpoint.location",
    s"/tmp/ekaterina.chechik/chk/lab04/state_${dateTimeNow}_${Random.nextInt(1000)}")

  val kafkaOptions: Map[String, String] =
    Map(
      "kafka.bootstrap.servers" -> kafkaTestHosts,
      "startingOffsets" -> kafkaTestStartingOffsets,
      "maxOffsetsPerTrigger" -> kafkaTestMaxOffsetsPerTrigger,
      "subscribe" -> kafkaTestInputTopic

    )

  case class Visit(timestamp: Long, url: String)
  case class TestData(uid: String, visits: Array[Visit])

  val testSchema: StructType =
    StructType(
      StructField("uid", StringType) ::
        StructField("visits", ArrayType(StructType(
          StructField("url", StringType) ::
            StructField("timestamp", LongType) :: Nil
        ))) :: Nil
    )

  val testDS: Dataset[TestData] = spark.readStream
    .format("kafka")
    .options(kafkaOptions)
    .load
    .select(
      from_json(col("value").cast(StringType), testSchema).as("json")
    )
    .select(
      testSchema.fields.map { field =>
        col(s"json.${field.name}").as(field.name)
      }: _*
    )
    .as[TestData]

  case class ClearedTestData(uid: String, domain: String, url: String)

  val clearedDS: Dataset[ClearedTestData] = testDS
    .withColumn("visits", explode_outer(col("visits")))
    .withColumn("pre_url" ,regexp_replace(
      regexp_replace(
        regexp_replace(col("visits.url"),
          "(http(s)?:\\/\\/https(:)?\\/\\/)", "https:\\/\\/"),
        "(http(s)?:\\/\\/http(:)?\\/\\/)", "http:\\/\\/"),
      "www\\.", "")
    )
    .withColumn("domain", lower(trim(callUDF("parse_url", col("pre_url"), lit("HOST")))))
    .withColumn("url", col("visits.url"))
    .drop("visits")
    .as[ClearedTestData]

  case class TestFeatures(uid: String, domains: Array[String])

  val featuresDS: Dataset[TestFeatures] =
    clearedDS
      .groupBy(col("uid"))
      .agg(
        collect_list(col("domain")).as("domains"),
        clearedDS.columns
          .filterNot(List("uid", "domain").contains(_))
          .map(nm => max(col(nm)).as(nm)): _*)
      .select(
        col("uid") +:
          clearedDS.columns
            .filterNot(List("uid", "domain").contains(_))
            .map(col) :+
          col("domains"): _*)
      .drop("timestamp")
      .as[TestFeatures]

  val model: PipelineModel = PipelineModel.load(hdfsModelPath)

  val predict: DataFrame = model.transform(featuresDS)

  val result: DataFrame = predict
    .select(
      to_json(struct(
        col("uid"),
        col("prediction_gender_age").as("gender_age")
      )).as("value")
    )

  result
    .writeStream
    .format("kafka")
    .outputMode(OutputMode.Update)
    .trigger(Trigger.ProcessingTime(5.seconds))
    .option("kafka.bootstrap.servers", kafkaTestHosts)
    .option("topic", kafkaTestOutputTopic)
    .option("checkpointLocation", kafkaCheckPointLocation)
    .start
    .awaitTermination(3.minutes.toMillis)

}

