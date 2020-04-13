package com.fsat.examples.spark.apps.apiusage.interpreters

import java.net.URI

import com.fsat.examples.spark.apps.apiusage.interpreters.ApiUsageAlgebra._
import org.apache.spark.sql.DataFrame

import scala.language.higherKinds

object ApiUsageAlgebra {
  final case class ProxyLog(df: DataFrame) extends AnyVal
  final case class Customers(df: DataFrame) extends AnyVal
  final case class ApiUsage(df: DataFrame) extends AnyVal
}

trait ApiUsageAlgebra[F[_]] {
  def loadProxyLog(): F[ProxyLog]
  def loadCustomers(): F[Customers]
  def deriveApiUsage(proxyLog: ProxyLog, customers: Customers): F[ApiUsage]
  def write(apiUsage: ApiUsage): F[URI]
}
