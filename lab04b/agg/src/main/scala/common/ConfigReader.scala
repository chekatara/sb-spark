package common

import java.io.InputStream
import scala.io.Source

object ConfigReader {
  def getParamValue(key: String): String = {

    val resourcesPath: InputStream = getClass.getResourceAsStream("/application.conf")
    val dict: Map[String, String] = Source.fromInputStream(resourcesPath).getLines
      .toList
      .map(x => x.split("="))
      //.map { case Array(f1,f2) => (f1,f2)}
      .filter(_.length == 2)
      .map { x =>
        x match {
          case Array(f1, f2) => (f1.trim, f2.trim)
        }
      }.toMap

    dict(key)
  }

  case class Conf(
                   kafkaInputServer: String,
                   kafkaInputSubscribe: String,
                   kafkaOutputServer: String,
                   kafkaOutputTopic: String,
                   kafkaCheckpointLocation: String
                 )

val conf: Conf = Conf(
  kafkaInputServer =  getParamValue("kafka-input-server"),
  kafkaInputSubscribe = getParamValue("kafka-input-subscribe"),
  kafkaOutputServer = getParamValue("kafka-output-server"),
  kafkaOutputTopic = getParamValue("kafka-output-topic"),
  kafkaCheckpointLocation = getParamValue("kafka-checkpoint-location")
)
}
