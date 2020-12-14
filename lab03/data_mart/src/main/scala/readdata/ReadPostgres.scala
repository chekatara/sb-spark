package readdata

import org.apache.spark.sql.{Dataset, SparkSession}

object ReadPostgres {
  case class CategoryInput(domain: Option[String], category: Option[String])

  def readCategory(pHost: String, pPort: String, pSchema: String,
                   pTable: String, pUser: String, pPassword: String,
                   spark: SparkSession): Dataset[CategoryInput] = {
    import spark.implicits._

    spark
      .read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$pHost:$pPort/$pSchema")
      .option("dbtable", pTable)
      .option("user", pUser)
      .option("password", pPassword)
      .option("driver", "org.postgresql.Driver")
      .load()
      .as[CategoryInput]
  }
}
