package com.fsat.examples.spark.apps.apiusage

import java.net.URI

import com.fsat.examples.spark.AppBase
import com.fsat.examples.spark.apps.apiusage.interpreters.ApiUsageAlgebra
import org.apache.spark.sql.SparkSession

import scala.util.Try

object ApiUsageApp extends AppBase {
  override def run(args: Seq[String])(implicit spark: SparkSession): Try[URI] = ???

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
