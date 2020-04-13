package com.fsat.examples.spark.apps.apiusage

import java.net.URI

import com.fsat.examples.spark.AppBase
import com.fsat.examples.spark.apps.apiusage.argparse.ApiUsageArgs
import com.fsat.examples.spark.apps.apiusage.interpreters.{ ApiUsageAlgebra, ApiUsageInterpreter }
import org.apache.spark.sql.SparkSession

import scala.util.{ Failure, Try }

object ApiUsageApp extends AppBase {
  override def run(args: Seq[String])(implicit spark: SparkSession): Try[URI] =
    ApiUsageArgs.parse(args) match {
      case Some(ApiUsageArgs(Some(proxyLogPath), Some(customerPath), Some(outputPath))) =>
        val interpreter = createInterpreter(proxyLogPath, customerPath, outputPath)
        execute(interpreter)

      case _ => Failure(new IllegalArgumentException(s"Invalid input argument for [${this.getClass.getSimpleName}] - ${args}"))
    }

  private[apiusage] def createInterpreter(proxyLogPath: URI, customerPath: URI, outputPath: URI)(implicit spark: SparkSession): ApiUsageAlgebra[Try] =
    new ApiUsageInterpreter(proxyLogPath, customerPath, outputPath)

  private[apiusage] def execute(interpreter: ApiUsageAlgebra[Try]): Try[URI] = {
    import interpreter._

    for {
      proxyLog <- loadProxyLog()
      customers <- loadCustomers()
      apiUsage <- deriveApiUsage(proxyLog, customers)
      savedPath <- write(apiUsage)
    } yield savedPath
  }
}
