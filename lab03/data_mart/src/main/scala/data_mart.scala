import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import common.ConfigReader.conf
import readdata.ReadCassandra.{readClients, ClientsInput}
import readdata.ReadEs.{readVisits, VisitsInput}
import readdata.ReadHdfs.{readWebLogs, WeblogsInput}
import readdata.ReadPostgres.{readCategory, CategoryInput}
import writedata.WritePostgres.writeClients
import udfs.UDFs

object data_mart extends App with Logging {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("Chechik_Ekaterina_lab03")
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  val clientsInput: Dataset[ClientsInput] = readClients(conf.srcCassandraHost,
                                                        conf.srcCassandraPort,
                                                        conf.srcCassandraKeyspace,
                                                        conf.srcCassandraTable,
                                                        spark)
  logInfo(s"clientsInput count: ${clientsInput.count}")
  logInfo(s"clientsInput schema: ${clientsInput.printSchema}")
  logInfo(s"clientsInput sample: ${clientsInput.take(10).mkString("\n")}")

  val clients: Dataset[Clients] = ageClients(clientsInput, spark)
  logInfo(s"clients count: ${clients.count}")
  logInfo(s"clients schema: ${clients.printSchema}")
  logInfo(s"clients sample: ${clients.take(10).mkString("\n")}")

  val visitsInput: Dataset[VisitsInput] = readVisits(conf.srcElasticsearchHost,
                                                    conf.srcElasticsearchPort,
                                                    conf.srcElasticsearchIndex,
                                                    spark)
  logInfo(s"visitsInput count: ${visitsInput.count}")
  logInfo(s"visitsInput schema: ${visitsInput.printSchema}")
  logInfo(s"visitsInput sample: ${visitsInput.take(10).mkString("\n")}")

  val visits: Dataset[Visits] = visitsShopCat(visitsInput, spark)
  logInfo(s"visits count: ${visits.count}")
  logInfo(s"visits schema: ${visits.printSchema}")
  logInfo(s"visits sample: ${visits.take(10).mkString("\n")}")

  val weblogsInput: Dataset[WeblogsInput] = readWebLogs(conf.srcHdfsPath,
                                                        conf.srcHdfsSep,
                                                        spark)
  logInfo(s"weblogsInput count: ${weblogsInput.count}")
  logInfo(s"weblogsInput schema: ${weblogsInput.printSchema}")
  logInfo(s"weblogsInput sample: ${weblogsInput.take(10).mkString("\n")}")

  val webLogs: Dataset[Weblogs] = weblogs(weblogsInput, spark)
  logInfo(s"webLogs count: ${webLogs.count}")
  logInfo(s"webLogs schema: ${webLogs.printSchema}")
  logInfo(s"webLogs sample: ${webLogs.take(10).mkString("\n")}")

  val weblogsDomain: Dataset[WeblogsDomain] = domain(webLogs, spark)
  logInfo(s"weblogsDomain count: ${weblogsDomain.count}")
  logInfo(s"weblogsDomain schema: ${weblogsDomain.printSchema}")
  logInfo(s"weblogsDomain sample: ${weblogsDomain.take(10).mkString("\n")}")

  val categoryInput: Dataset[CategoryInput] = readCategory(conf.srcPostgresHost,
                                                           conf.srcPostgresPort,
                                                           conf.srcPostgresSchema,
                                                           conf.srcPostgresTable,
                                                           conf.postgresUser,
                                                           conf.postgressPassword,
                                                           spark)
  logInfo(s"categoryInput count: ${categoryInput.count}")
  logInfo(s"categoryInput schema: ${categoryInput.printSchema}")
  logInfo(s"categoryInput sample: ${categoryInput.take(10).mkString("\n")}")

  val category: Dataset[Category] = webCategory(categoryInput, spark)
  logInfo(s"category count: ${category.count}")
  logInfo(s"category schema: ${category.printSchema}")
  logInfo(s"category sample: ${category.take(10).mkString("\n")}")

  val webPivot: DataFrame = webPivot(weblogsDomain, category, spark)
  logInfo(s"webPivot count: ${webPivot.count}")
  logInfo(s"webPivot schema: ${webPivot.printSchema}")
  logInfo(s"webPivot sample: ${webPivot.take(10).mkString("\n")}")

  val shopPivot: DataFrame = shopPivot(visits, spark)
  logInfo(s"shopPivot count: ${shopPivot.count}")
  logInfo(s"shopPivot schema: ${shopPivot.printSchema}")
  logInfo(s"shopPivot sample: ${shopPivot.take(10).mkString("\n")}")

  val clientsToPostgres: DataFrame = clientsToPostgres(clients, shopPivot, webPivot, spark)
  logInfo(s"clientsToPostgres count: ${clientsToPostgres.count}")
  logInfo(s"clientsToPostgres schema: ${clientsToPostgres.printSchema}")
  logInfo(s"clientsToPostgres sample: ${clientsToPostgres.take(10).mkString("\n")}")

  writeClients(clientsToPostgres, conf.tgtPostgresHost, conf.tgtPostgresPort, conf.tgtPostgresSchema,
              conf.tgtPostgresTable, conf.postgresUser, conf.postgressPassword, spark)

  case class Clients(uid: String, gender: String, age_cat: String)

  def ageClients(clInput: Dataset[ClientsInput],
                   spark: SparkSession): Dataset[Clients] = {
   import spark.implicits._

      clInput
        .withColumn("age_cat",
          when(col("age").between(18,24), "18-24")
            .when(col("age").between(25,34), "25-34")
            .when(col("age").between(35,44), "35-44")
            .when(col("age").between(45,54), "45-54")
            .otherwise(">=55")
        ).drop("age")
        .as[Clients]
    }

  case class Visits(category: String,
                    event_type: String,
                    item_id: String,
                    item_price: Long,
                    timestamp: String,
                    uid: String,
                    category_clear: String,
                    shop_cat: String)

  def visitsShopCat(visInput: Dataset[VisitsInput], spark: SparkSession): Dataset[Visits] = {
    import spark.implicits._

    visInput
      .withColumn("category_clear", regexp_replace(col("category"), "[ ]|[-]", "_"))
      .withColumn("shop", lit("shop_"))
      .withColumn("shop_cat", lower(concat(col("shop"), col("category_clear"))))
      .drop("shop")
      .as[Visits]
  }

  case class Weblogs(uid: String, timestamp: Long, url: String)

  def weblogs(logs: Dataset[WeblogsInput], spark: SparkSession): Dataset[Weblogs] = {
    import spark.implicits._

    logs
      .filter(col("visits").isNotNull)
      .withColumn("tmp", explode(col("visits")))
      .withColumn("timestamp", col("tmp.timestamp"))
      .withColumn("url", col("tmp.url"))
      .drop("visits")
      .drop("tmp")
      .as[Weblogs]
  }

  case class WeblogsDomain(uid: String,
                           timestamp: Long,
                           url: String,
                           host: String,
                           domain: String)

  def domain (weblogs: Dataset[Weblogs], spark: SparkSession): Dataset[WeblogsDomain] = {
    import spark.implicits._

     weblogs
      .withColumn("host", UDFs.hostUDF(col("url")))
      .withColumn("domain", regexp_replace(col("host"), "^www.", ""))
      .as[WeblogsDomain]
  }

  case class Category(domain: String, category: String, web_cat: String)

  def webCategory(categoryInput: Dataset[CategoryInput],
                  spark: SparkSession): Dataset[Category] = {
    import spark.implicits._

    categoryInput
      .withColumn("web", lit("web_"))
      .withColumn("web_cat", concat(col("web"), col("category")))
      .drop("web")
      .as[Category]
  }

  def webPivot (weblogsDomain: Dataset[WeblogsDomain],
                category: Dataset[Category],
                spark: SparkSession): DataFrame = {
    import spark.implicits._

    weblogsDomain
      .join(category, Seq("domain"), "inner")
      .filter('uid.isNotNull)
      .filter('category.isNotNull)
      .groupBy('uid, 'web_cat).agg(count("*").as("cnt"))
      .groupBy('uid).pivot('web_cat).agg(sum('cnt))
  }

  def shopPivot(visits: Dataset[Visits], spark:SparkSession): DataFrame = {
    import spark.implicits._

    visits
      .filter('uid.isNotNull)
      .filter('category.isNotNull)
      .groupBy('uid, 'shop_cat).agg(count("*").as("cnt"))
      .groupBy('uid).pivot('shop_cat).agg(sum('cnt))
  }

  def clientsToPostgres(clients: Dataset[Clients],
                        shopPivot: DataFrame,
                        webPivot: DataFrame,
                        spark: SparkSession): DataFrame = {
    import spark.implicits._

    val pre = clients
      .join(shopPivot, Seq("uid"), "left")
      .join(webPivot, Seq("uid"), "left")

    pre.select(
      pre.columns.map{
          case n =>
            if (n.startsWith("web_") || n.startsWith("shop_"))
              coalesce(col(n), lit(0)).as(n)
            else col(n)
        }: _*
      )
  }

  println("*" * 60)
  println(" " * 28 + "DONE" + " " * 28)
  println("*" * 60)
}