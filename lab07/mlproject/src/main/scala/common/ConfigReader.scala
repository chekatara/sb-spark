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
                   hdfsDataPath: String,
                   hdfsModelPath: String,
                   kafkaTestHosts: String,
                   kafkaTestStartingOffsets: String,
                   kafkaTestMaxOffsetsPerTrigger: String,
                   kafkaTestInputTopic: String,
                   kafkaTestOutputTopic: String
                 )

  val conf: Conf = Conf (
    hdfsDataPath = getParamValue("hdfs-data-path"),
    hdfsModelPath = getParamValue("hdfs-model-path"),
    kafkaTestHosts = getParamValue("kafka-test-hosts"),
    kafkaTestStartingOffsets = getParamValue("kafka-test-starting-offsets"),
    kafkaTestMaxOffsetsPerTrigger = getParamValue("kafka-test-max-offsets-per-trigger"),
    kafkaTestInputTopic = getParamValue("kafka-test-input-topic"),
    kafkaTestOutputTopic = getParamValue("kafka-test-output-topic")
  )
}