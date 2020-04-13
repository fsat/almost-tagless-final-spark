package com.fsat.examples.spark.apps.apiusage.argparse

import java.net.URI
import scopt.OParser

object ApiUsageArgs {
  private val parser = {
    val builder = OParser.builder[ApiUsageArgs]
    import builder._

    OParser.sequence(
      programName("api-usage"),
      opt[URI]("proxy-log-path")
        .action((v, args) => args.copy(proxyLogPath = Some(v))),
      opt[URI]("customers-path")
        .action((v, args) => args.copy(customersPath = Some(v))),
      opt[URI]("output-path")
        .action((v, args) => args.copy(outputPath = Some(v))))
  }

  def parse(args: Seq[String]): Option[ApiUsageArgs] = OParser.parse(parser, args, ApiUsageArgs())
}

final case class ApiUsageArgs(
  proxyLogPath: Option[URI] = None,
  customersPath: Option[URI] = None,
  outputPath: Option[URI] = None)
