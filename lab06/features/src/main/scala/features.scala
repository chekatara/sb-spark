import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

object features extends App{
lazy val spark: SparkSession = SparkSession.builder().appName("ekaterina.chechik_features").getOrCreate()

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  import spark.implicits._

  val jsonSchema = StructType(
    List(StructField("uid", StringType),
      StructField("visits", ArrayType(StructType(List(
        StructField("timestamp", LongType),
        StructField("url", StringType)
      ))))
    )
  )

  val weblogs_input_df = spark
    .read
    .schema(jsonSchema)
    .option("header", "false")
    .option("sep", "\t")
    .json("hdfs:///labs/laba03/weblogs.json")

  val weblogs_df = weblogs_input_df.filter(col("visits").isNotNull)
    .withColumn("tmp", explode(col("visits")))
    .withColumn("timestamp", col("tmp.timestamp"))
    .withColumn("urlInput", col("tmp.url"))
    .withColumn("host", lower(callUDF("parse_url", 'urlInput, lit("HOST"))))
    .withColumn("domain", regexp_replace('host, "www.", ""))
    .drop("visits")
    .drop("tmp")
    .drop("urlInput")
    .withColumn ("datetime", date_format(('timestamp/1000).cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("weekday", lower(date_format('datetime, "E")))
    .withColumn("hour", date_format('datetime, "H"))
    .withColumn("work_hours_flg", when ('hour >= 9 and 'hour < 18, 1).otherwise(0))
    .withColumn("evening_hours_flg", when ('hour >= 18 and 'hour < 24, 1).otherwise(0))
    .filter(col("domain").isNotNull)
    .cache()

  weblogs_df.count

  val weblogs_top_df = weblogs_df.groupBy('domain).agg(count("*").as("cnt"))
    .withColumn("tmp", lit(1))
    .withColumn ("rn", row_number().over(Window.partitionBy('tmp).orderBy('cnt.desc)))
    .drop('tmp)
    .filter('rn <= 1000)
    .orderBy('domain)
    .cache

  weblogs_top_df.count

  val pre_pivot_weblogs_weekday_df = weblogs_df.withColumn("web_day", lit("web_day_"))
    .withColumn("weekday", concat('web_day, 'weekday))
    .groupBy('uid, 'weekday).agg(count("*").as("cnt"))
    .groupBy('uid).pivot('weekday).agg(sum('cnt))

  val pivot_weblogs_weekday_df = pre_pivot_weblogs_weekday_df.select(
    pre_pivot_weblogs_weekday_df.columns.map{
      case n =>
        coalesce(col(n), lit(0)).as(n)
    }: _*
  )

  val pre_pivot_weblogs_hour_df = weblogs_df.withColumn("web_hour", lit("web_hour_"))
    .withColumn("hour", concat('web_hour, 'hour))
    .groupBy('uid, 'hour).agg(count("*").as("cnt"))
    .groupBy('uid).pivot('hour).agg(sum('cnt))

  val pivot_weblogs_hour_df = pre_pivot_weblogs_hour_df.select(
    pre_pivot_weblogs_hour_df.columns.map{
      case n =>
        coalesce(col(n), lit(0)).as(n)
    }: _*
  )

  val pivot_weblogs_work_evening_df = weblogs_df.groupBy('uid)
    .agg(sum('work_hours_flg).as("web_work_hours"),
      sum('evening_hours_flg).as("web_evening_hours"),
      count("*").as("web_all"))
    .withColumn("web_fraction_work_hours", 'web_work_hours / 'web_all)
    .withColumn("web_fraction_evening_hours", 'web_evening_hours / 'web_all)
    .drop("web_work_hours", "web_evening_hours", "web_all")

  val pre_pivot_weblogs_domain_df = weblogs_df.join(weblogs_top_df, Seq("domain"), "left")
    .withColumn("cnt_log", when('rn.isNotNull, lit(1)).otherwise(lit(0)))
    .withColumn("domain", when('rn.isNotNull, 'domain).otherwise(lit("nan")))
    .groupBy('uid, 'domain).agg(sum("cnt_log").as("cnt_pivot"))
    .orderBy('domain)
    .groupBy('uid).pivot('domain).agg(sum('cnt_pivot))

  val pivot_weblogs_domain_df = pre_pivot_weblogs_domain_df.select(
    pre_pivot_weblogs_domain_df.columns.map{
      case n =>
        coalesce(col(s"`$n`"), lit(0)).as(n)
    }: _*
  )

  val arr_weblogs_domain_df = pivot_weblogs_domain_df.select(
    'uid,
    array(pivot_weblogs_domain_df.columns.filter(_ != "uid").filter(_ != "nan").map{
      case n =>
        col(s"`$n`")
    }: _*).as("domain_features")
  )

  val final_web_df = arr_weblogs_domain_df.join(pivot_weblogs_weekday_df, Seq("uid"), "inner")
    .join(pivot_weblogs_hour_df, Seq("uid"), "inner")
    .join(pivot_weblogs_work_evening_df, Seq("uid"), "inner")

  val users_items_df = spark.read
    .option("header", "false")
    .option("path", "/user/ekaterina.chechik/users-items/20200429")
    .load()

  val final_df = final_web_df.join(users_items_df, Seq("uid"), "left")

  final_df
    .write
    .format("parquet")
    .mode("overwrite")
    .option("path", "/user/ekaterina.chechik/features")
    .save()

}
