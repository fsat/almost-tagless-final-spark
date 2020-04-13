package com.fsat.examples.spark.apps.apiusage.interpreters

import java.nio.file.{ Path, Paths }

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
            api_path = Some("/search"),
            customer_id = Some(1L),
            count = Some(2),
            date = Some(Dates.sqlDate(2020, 3, 1))))

        val inputDF = spark.createDataFrame(input)

        val result = interpreter.write(ApiUsageAlgebra.ApiUsage(inputDF)).get

        Paths.get(result) shouldBe outputPath

        val savedData = spark.read.parquet(outputPath.toAbsolutePath.toString)

        savedData.as[ApiUsageF].collect() shouldBe input

      }
    }
  }

  describe("deriveApiUsage") {
    it("groups api usage by path, customer, and date") {
      withTempDir() { tmpDir =>
        import spark.sqlContext.implicits._

        val f = testFixture(tmpDir)
        import f._

        val proxyLogs = Seq(
          ProxyLogF(
            request_path = Some("/api/v2/search/customers?query=name:john"),
            domain = Some("www.customer.com"),
            timestamp_epoch = Some(Dates.timestamp(2020, 3, 1, 15, 12, 0))),
          ProxyLogF(
            request_path = Some("/api/v2/search/items?query=name:john"),
            domain = Some("www.customer.com"),
            timestamp_epoch = Some(Dates.timestamp(2020, 3, 1, 23, 5, 10))),
          ProxyLogF(
            request_path = Some("/api/v2/catalogue"),
            domain = Some("other-customer.biz"),
            timestamp_epoch = Some(Dates.timestamp(2020, 3, 10, 11, 57, 23))),
          ProxyLogF(
            request_path = Some("/images/blank.gif"),
            domain = None,
            timestamp_epoch = Some(Dates.timestamp(2020, 3, 10, 10, 3, 33))))
        val proxyLogsDF = spark.createDataFrame(proxyLogs)

        val customers = Seq(
          CustomerF(
            customer_id = Some(1),
            domain = Some("www.customer.com")),
          CustomerF(
            customer_id = Some(10),
            domain = Some("other-customer.biz")))
        val customersDF = spark.createDataFrame(customers)

        val result = interpreter.deriveApiUsage(
          ApiUsageAlgebra.ProxyLog(proxyLogsDF),
          ApiUsageAlgebra.Customers(customersDF)).get

        val expectedResult = Seq(
          ApiUsageF(
            api_path = Some("search"),
            customer_id = Some(1),
            count = Some(2),
            date = Some(Dates.sqlDate(2020, 3, 1))),
          ApiUsageF(
            api_path = Some("catalogue"),
            customer_id = Some(10),
            count = Some(1),
            date = Some(Dates.sqlDate(2020, 3, 10))))

        def apiUsageSort(apiUsageF: ApiUsageF): (Option[String], Option[Long], Option[Long], Option[Long]) =
          (apiUsageF.api_path, apiUsageF.customer_id, apiUsageF.count, apiUsageF.date.map(_.getTime))

        result.df.as[ApiUsageF].collect().sortBy(apiUsageSort) shouldBe expectedResult.sortBy(apiUsageSort)
      }
    }
  }

  describe("apiPath") {
    it("returns correct api path") {
      val result = ApiUsageInterpreter.apiPath("/api/v2/search/customers?terms=name:joe")
      result shouldBe Some("search")
    }

    it("returns none") {
      val result = ApiUsageInterpreter.apiPath("/images/blank.gif")
      result.isEmpty shouldBe true
    }
  }

  describe("toDayStart") {
    it("returns date at the beginning of the day") {
      val timestamp = Dates.timestamp(2020, 3, 10, 10, 3, 33)

      val result = ApiUsageInterpreter.toDayStart(timestamp)
      result shouldBe Dates.sqlDate(2020, 3, 10)
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
