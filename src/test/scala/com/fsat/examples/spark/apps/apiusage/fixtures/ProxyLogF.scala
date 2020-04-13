package com.fsat.examples.spark.apps.apiusage.fixtures

import java.sql.Timestamp

final case class ProxyLogF(
  request_path: Option[String] = None,
  domain: Option[String] = None,
  timestamp_epoch: Option[Timestamp] = None)
