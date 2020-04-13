package com.fsat.examples.spark.apps.apiusage.interpreters

import java.net.URI

import org.apache.spark.sql.SparkSession

import scala.util.Try

class ApiUsageInterpreter(
  proxyLogPath: URI,
  customersPath: URI,
  outputPath: URI
)(implicit spark: SparkSession) extends ApiUsageAlgebra[Try] {
  override def loadProxyLog(): Try[ApiUsageAlgebra.ProxyLog] = ???
  override def loadCustomers(): Try[ApiUsageAlgebra.Customers] = ???
  override def deriveApiUsage(proxyLog: ApiUsageAlgebra.ProxyLog, customers: ApiUsageAlgebra.Customers): Try[ApiUsageAlgebra.ApiUsage] = ???
  override def write(apiUsage: ApiUsageAlgebra.ApiUsage): Try[URI] = ???
}
