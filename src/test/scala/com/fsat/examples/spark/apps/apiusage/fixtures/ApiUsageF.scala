package com.fsat.examples.spark.apps.apiusage.fixtures

import java.sql.Date

final case class ApiUsageF(
  api_path: Option[String] = None,
  customer_id: Option[Long] = None,
  count: Option[Long] = None,
  date: Option[Date] = None)
