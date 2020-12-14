package writedata

object WritePostgres {
  import org.apache.spark.sql.{DataFrame, SparkSession}

  def writeClients(df: DataFrame, pHost: String, pPort: String, pSchema: String,
                   pTrgTable: String, pUser: String, pPassword: String,
                   spark: SparkSession
                  ): Unit = {
    import spark.implicits._

    df.write
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$pHost:$pPort/$pSchema")
      .option("dbtable", pTrgTable)
      .option("user", pUser)
      .option("password", pPassword)
      .option("driver", "org.postgresql.Driver")
      .save()
  }

}
