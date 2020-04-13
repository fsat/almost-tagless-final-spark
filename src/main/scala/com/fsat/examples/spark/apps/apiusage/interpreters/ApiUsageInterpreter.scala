package com.fsat.examples.spark.apps.apiusage.interpreters

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.util.Try

class ApiUsageInterpreter(
  proxyLogPath: URI,
  customersPath: URI,
  outputPath: URI)(implicit spark: SparkSession) extends ApiUsageAlgebra[Try] with LazyLogging {

  override def loadProxyLog(): Try[ApiUsageAlgebra.ProxyLog] =
    Try {
      logger.info(s"Loading proxy log from [$proxyLogPath]")
      ApiUsageAlgebra.ProxyLog(spark.read.parquet(proxyLogPath.toString))
    }

  override def loadCustomers(): Try[ApiUsageAlgebra.Customers] =
    Try {
      logger.info(s"Loading customers from [$customersPath]")
      ApiUsageAlgebra.Customers(spark.read.parquet(customersPath.toString))
    }

  override def deriveApiUsage(proxyLog: ApiUsageAlgebra.ProxyLog, customers: ApiUsageAlgebra.Customers): Try[ApiUsageAlgebra.ApiUsage] = ???

  override def write(apiUsage: ApiUsageAlgebra.ApiUsage): Try[URI] =
    Try {
      logger.info(s"Saving api usage data to [$outputPath]")
      apiUsage.df.write.parquet(outputPath.toString)
      outputPath
    }
}
