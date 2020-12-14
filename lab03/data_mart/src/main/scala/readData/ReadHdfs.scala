package readData

import org.apache.spark.sql.{Dataset, SparkSession}

object ReadHdfs{
  case class WeblogsInput(uid: Option[String], visits: Option[Array[(Long, String)]])

  def readWebLogs(hdfsPath: String,
                  hdfsSep: String,
                  spark: SparkSession): Dataset[WeblogsInput] = {
    import spark.implicits._

    spark
      .read
      .option("header", "false")
      .option("sep", hdfsSep)
      .json(hdfsPath)
      .as[WeblogsInput]

  }

}