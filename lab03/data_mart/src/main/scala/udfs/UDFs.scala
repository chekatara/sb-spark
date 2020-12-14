package udfs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.util.{Try}
import org.apache.spark.sql.{SparkSession}

object UDFs extends Serializable {

  val hostUDF: UserDefinedFunction = udf(
    (x:String) => {
      Try(new java.net.URL(x).getHost).toOption
    })

}
