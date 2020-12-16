import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object filter extends App{

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("Chechik_Ekaterina_lab04a")
    .getOrCreate()

  import spark.implicits._

  val topic: String = spark.conf.get("spark.filter.topic_name", "lab04_input_data")
  val offset: String = spark.conf.get("spark.filter.offset", "earliest")
  val outputDir: String = spark.conf.get("spark.filter.output_dir_prefix",
                                        "/user/ekaterina.chechik/visits")

  val offsetRes = Try(offset.toInt)
   match {
    case Success(v) => s""" { "$topic": { "0": $v } }  """
    case Failure(_) =>
      offset
  }

  val inputData: DataFrame = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", topic)
    .option("startingOffsets", offsetRes)
    .load()

  val jsonString: Dataset[String]  = inputData
    .select(
      col("value").cast("string")
    ).as[String]

  val df = spark
    .read
    .json(jsonString)
    .withColumn ("date", to_date(('timestamp/1000).cast("timestamp"), "yyyyMMdd"))
    .withColumn("p_date", 'date)

  val viewDf = df
    .filter('event_type === "view")

  val buyDf =  df
    .filter('event_type === "buy")

  viewDf
    .write
    .format("json")
    .mode("overwrite")
    .partitionBy("p_date")
    .option("path", s"$outputDir/view")
    .save()

  buyDf
    .write
    .format("json")
    .mode("overwrite")
    .partitionBy("p_date")
    .option("path", s"$outputDir/buy")
    .save()
}
