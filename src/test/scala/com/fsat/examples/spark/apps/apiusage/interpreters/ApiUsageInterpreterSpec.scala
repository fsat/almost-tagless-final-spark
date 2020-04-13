package com.fsat.examples.spark.apps.apiusage.interpreters

import java.nio.file.{ Path, Paths }
import java.sql.{ Date, Timestamp }

import com.fsat.examples.spark.SparkTestBase
import com.fsat.examples.spark.SparkTestBase.{ Dates, withTempDir }
import com.fsat.examples.spark.apps.apiusage.fixtures.{ ApiUsageF, CustomerF, ProxyLogF }

class ApiUsageInterpreterSpec extends SparkTestBase {
  describe("loadProxyLog") {
    it("loads the proxy logs") {
      withTempDir() { tmpDir =>
        import spark.sqlContext.implicits._

        val f = testFixture(tmpDir)
        import f._

        val input = Seq(
          ProxyLogF(
            request_path = Some("/api/v2/search"),
            domain = Some("custom.com"),
            timestamp_epoch = Some(Dates.timestamp(2020, 3, 1))))
        val inputDF = spark.createDataFrame(input)

        inputDF.write.parquet(proxyLogPath.toAbsolutePath.toString)

        val loadedData = interpreter.loadProxyLog().get

        loadedData.df.as[ProxyLogF].collect() shouldBe input
      }
    }
  }

  describe("loadCustomers") {
    it("loads the customers data") {
      withTempDir() { tmpDir =>
        import spark.sqlContext.implicits._

        val f = testFixture(tmpDir)
        import f._

        val input = Seq(
          CustomerF(
            customer_id = Some(10),
            domain = Some("custom.com")))
        val inputDF = spark.createDataFrame(input)

        inputDF.write.parquet(customersPath.toAbsolutePath.toString)

        val loadedData = interpreter.loadCustomers().get

        loadedData.df.as[CustomerF].collect() shouldBe input
      }
    }
  }

  describe("write") {
    it("saves the api usage data") {
      withTempDir() { tmpDir =>
        import spark.sqlContext.implicits._

        val f = testFixture(tmpDir)
        import f._

        val input = Seq(
          ApiUsageF(
            apiPath = Some("/search"),
            customer_id = Some(1L),
            date = Some(Dates.sqlDate(2020, 3, 1))))

        val inputDF = spark.createDataFrame(input)

        val result = interpreter.write(ApiUsageAlgebra.ApiUsage(inputDF)).get

        Paths.get(result) shouldBe outputPath

        val savedData = spark.read.parquet(outputPath.toAbsolutePath.toString)

        savedData.as[ApiUsageF].collect() shouldBe input

      }
    }
  }

  private def testFixture(tmpDir: Path) = new {
    val proxyLogPath = tmpDir.resolve("proxy-logs")
    val customersPath = tmpDir.resolve("customers")
    val outputPath = tmpDir.resolve("api-usage")

    val interpreter = new ApiUsageInterpreter(
      proxyLogPath = proxyLogPath.toUri,
      customersPath = customersPath.toUri,
      outputPath = outputPath.toUri)
  }
}
