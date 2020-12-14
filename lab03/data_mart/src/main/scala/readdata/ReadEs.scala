package readdata

import org.apache.spark.sql.{Dataset, SparkSession}

object ReadEs{
  case class VisitsInput(category: Option[String],
                         event_type: Option[String],
                         item_id: Option[String],
                         item_price: Option[Long],
                         timestamp: Option[String],
                         uid: Option[String])

  def readVisits(esHost: String,
                 esPort: String,
                 esIndex: String,
                 spark: SparkSession): Dataset[VisitsInput] = {
    import spark.implicits._

    spark
      .read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", esHost).option("es.port", esPort)
      .option("es.batch.write.refresh", "false")
      .option("es.nodes.wan.only", "true")
      .load(esIndex)
      .as[VisitsInput]
  }
}