package com.fsat.examples.spark.apps.apiusage.interpreters

import java.net.URI
import java.sql.{ Date, Timestamp }
import java.time.{ Instant, ZoneId, ZonedDateTime }

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Try

object ApiUsageInterpreter {
  def apiPath(input: String): Option[String] =
    for {
      v <- Option(input)
      parts = v.split("/").toSeq

      firstPart <- parts.drop(1).headOption
      secondPart <- parts.drop(2).headOption
      result <- if (firstPart == "api" && secondPart.startsWith("v"))
        parts.drop(3).headOption
      else None
    } yield result

  def toDayStart(timestamp: Timestamp): Date = {
    val utcDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getTime), ZoneId.of("UTC"))
    Date.valueOf(utcDateTime.toLocalDate)
  }
}

class ApiUsageInterpreter(
  proxyLogPath: URI,
  customersPath: URI,
  outputPath: URI)(implicit spark: SparkSession) extends ApiUsageAlgebra[Try] with LazyLogging {

  import ApiUsageInterpreter._

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

  override def deriveApiUsage(proxyLog: ApiUsageAlgebra.ProxyLog, customers: ApiUsageAlgebra.Customers): Try[ApiUsageAlgebra.ApiUsage] =
    Try {
      val apiPathUdf = udf(apiPath _)
      val toDayStartUdf = udf(toDayStart _)

      val proxyLogDF = proxyLog.df
      val customersDF = customers.df

      val result =
        proxyLogDF
          .withColumn("date", toDayStartUdf(col("timestamp_epoch")))
          .withColumn("api_path", apiPathUdf(col("request_path")))
          .filter(col("api_path").isNotNull)
          .join(customersDF, "domain")
          .select("api_path", "customer_id", "date")
          .groupBy("api_path", "customer_id", "date")
          .agg(count(col("customer_id")).as("count"))

      ApiUsageAlgebra.ApiUsage(result)
    }

  override def write(apiUsage: ApiUsageAlgebra.ApiUsage): Try[URI] =
    Try {
      logger.info(s"Saving api usage data to [$outputPath]")
      apiUsage.df.write.parquet(outputPath.toString)
      outputPath
    }
}
