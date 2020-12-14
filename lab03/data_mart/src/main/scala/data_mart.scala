import common.ConfigReader.conf
import readData.ReadCassandra.{ClientsInput, readClients}
import readData.ReadEs.{VisitsInput, readVisits}
import readData.ReadHdfs.{WeblogsInput, readWebLogs}
import readData.ReadPostgres.{CategoryInput, readCategory}
import writeData.WritePostgres.writeClients
import UDFs.UDFs
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object data_mart extends App{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("lab03")
    .getOrCreate()

  val clientsInput: Dataset[ClientsInput] = readClients(conf.srcCassandraHost,
                                                        conf.srcCassandraPort,
                                                        conf.srcCassandraKeyspace,
                                                        conf.srcCassandraTable,
                                                        spark)

  val clients: Dataset[Clients] = ageClients(clientsInput, spark)

  val visitsInput: Dataset[VisitsInput] = readVisits(conf.srcElasticsearchHost,
                                                    conf.srcElasticsearchPort,
                                                    conf.srcElasticsearchIndex,
                                                    spark)

  val visits: Dataset[Visits] = visitsShopCat(visitsInput, spark)

  val weblogsInput: Dataset[WeblogsInput] = readWebLogs(conf.srcHdfsPath,
                                                        conf.srcHdfsSep,
                                                        spark)

  val webLogs: Dataset[Weblogs] = weblogs(weblogsInput, spark)

  val weblogsDomain: Dataset[WeblogsDomain] = domain(webLogs, spark)

  val categoryInput: Dataset[CategoryInput] = readCategory(conf.srcPostgresHost,
                                                           conf.srcPostgresPort,
                                                           conf.srcPostgresSchema,
                                                           conf.srcPostgresTable,
                                                           conf.postgresUser,
                                                           conf.postgressPassword,
                                                           spark)

  val category: Dataset[Category] = webCategory(categoryInput, spark)

  val webPivot: DataFrame = webPivot(weblogsDomain, category, spark)

  val shopPivot: DataFrame = shopPivot(visits, spark)

  val clientsToPostgres: DataFrame = clientsToPostgres(clients, shopPivot, webPivot, spark)

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
      //.withColumn("host", col("url"))
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

  println("**************************************************************")
  println("!!!!!!!!!!!!!!!!!!!!!!!!!!!DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  println("**************************************************************")

}