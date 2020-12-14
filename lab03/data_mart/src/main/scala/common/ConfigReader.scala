package common

import java.io.InputStream
import scala.io.Source


object ConfigReader {

  def getParamValue (arg:String): String = {

    val resourcesPath: InputStream = getClass.getResourceAsStream("/application.conf")
    val dict = Source.fromInputStream(resourcesPath).getLines
      .toList
      .map(x => x.split("="))
      //.map { case Array(f1,f2) => (f1,f2)}
      .filter(_.length == 2)
      .map { x =>
        x match {
          case Array(f1, f2) => (f1.trim, f2.trim)
        }
      }.toMap

    dict(arg)
  }

  case class Conf(
                   srcCassandraHost: String,
                   srcCassandraPort: String,
                   srcCassandraKeyspace: String,
                   srcCassandraTable: String,
                   srcElasticsearchHost: String,
                   srcElasticsearchPort: String,
                   srcElasticsearchIndex: String,
                   srcHdfsPath: String,
                   srcHdfsSep: String,
                   srcPostgresHost: String,
                   srcPostgresPort: String,
                   srcPostgresSchema: String,
                   srcPostgresTable: String,
                   tgtPostgresHost: String,
                   tgtPostgresPort: String,
                   tgtPostgresSchema: String,
                   tgtPostgresTable: String,
                   postgresUser: String,
                   postgressPassword: String
                 )

  val conf = Conf (
    srcCassandraHost = getParamValue("src-cassandra-host"),
    srcCassandraPort = getParamValue("src-cassandra-port"),
    srcCassandraKeyspace = getParamValue("src-cassandra-keyspace"),
    srcCassandraTable = getParamValue("src-cassandra-table"),
    srcElasticsearchHost = getParamValue("src-elasticsearch-host"),
    srcElasticsearchPort = getParamValue("src-elasticsearch-port"),
    srcElasticsearchIndex = getParamValue("src-elasticsearch-index"),
    srcHdfsPath = getParamValue("src-hdfs-path"),
    srcHdfsSep = getParamValue("src-hdfs-sep"),
    srcPostgresHost = getParamValue("src-postgres-host"),
    srcPostgresPort = getParamValue("src-postgres-port"),
    srcPostgresSchema = getParamValue("src-postgres-schema"),
    srcPostgresTable = getParamValue("src-postgres-table"),
    tgtPostgresHost = getParamValue("tgt-postgres-host"),
    tgtPostgresPort = getParamValue("tgt-postgres-port"),
    tgtPostgresSchema = getParamValue("tgt-postgres-schema"),
    tgtPostgresTable = getParamValue("tgt-postgres-table"),
    postgresUser = getParamValue("postgres-user"),
    postgressPassword = getParamValue("postgress-password")
  )
}