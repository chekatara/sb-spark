import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object users_items extends App {

  lazy val spark: SparkSession = SparkSession.builder().appName("Ekaterina_Chechik_lab05").getOrCreate()

  import spark.implicits._

  val inputDir: String = spark.conf.get("spark.users_items.input_dir",
                                        "/user/ekaterina.chechik/visits")

  val outputDir: String = spark.conf.get("spark.users_items.output_dir",
                                          "/user/ekaterina.chechik/users-items")

  val updMode: String = spark.conf.get("spark.users_items.update", "0")

  val buyDf: DataFrame = spark.read
    .option("header", "true")
    .json(s"${inputDir}/buy")

  val viewDf: DataFrame = spark.read
    .option("header", "true")
    .json(s"${inputDir}/view")

  val df: DataFrame = List(buyDf, viewDf).reduce(_ union _)

  val maxDate: String = df.select(max(date_add(to_date('p_date, "yyyyMMdd"), -1)).as("maxDt"))
    .withColumn("maxDt", date_format('maxDt, "yyyyMMdd"))
    .collect
    .mkString(",")
    .replaceAll("[\\[\\]]","")

  val pivotDfPre: DataFrame =  df
    .filter('uid.isNotNull)
    .filter('item_id.isNotNull)
    .withColumn("item_id_clear", lower(regexp_replace(col("item_id"), "[ ]|[-]", "_")))
    .withColumn("underline", lit("_"))
    .withColumn("item4pivot", concat('event_type, 'underline, 'item_id_clear))
    .cache
    .groupBy('uid, 'item4pivot).agg(count("*").as("cnt"))
    .groupBy('uid).pivot('item4pivot).agg(sum('cnt))

  val pivotDf: DataFrame = pivotDfPre.select(
    pivotDfPre.columns.map{
      case n =>
        if (n.startsWith("view_") || n.startsWith("buy_"))
          coalesce(col(n), lit(0)).as(n)
        else col(n)
    }: _*)

  pivotDf.write
    .format("parquet")
    .option("path", s"${outputDir}/$maxDate")
    .mode("overwrite")
    .save()

}
