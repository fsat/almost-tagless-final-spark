package com.fsat.examples.spark.apps.apiusage.argparse

import java.net.URI

import com.fsat.examples.spark.TestBase

class ApiUsageArgsSpec extends TestBase {
  it("parses correct arguments") {
    val result = ApiUsageArgs.parse(Seq(
      "--proxy-log-path", "/proxy-log-path",
      "--customers-path", "/customers-path",
      "--output-path", "/output-path"))

    val expectedResult = ApiUsageArgs(
      proxyLogPath = Some(new URI("/proxy-log-path")),
      customersPath = Some(new URI("/customers-path")),
      outputPath = Some(new URI("/output-path")))

    result.get shouldBe expectedResult
  }
}
