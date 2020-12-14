package readData

import org.apache.spark.sql.{Dataset, SparkSession}

object ReadCassandra{

  case class ClientsInput(uid: Option[String], age: Option[Int], gender: Option[String])

  def readClients (casHost: String,
                   casPort: String,
                   casKeyspace: String,
                   casTable: String,
                   spark: SparkSession): Dataset[ClientsInput] = {
    import spark.implicits._

    spark.conf.set("spark.cassandra.connection.host", casHost)
    spark.conf.set("spark.cassandra.connection.port", casPort)
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", casKeyspace)
      .option("table", casTable)
      .load()
      .as[ClientsInput]
  }
}