package com.fsat.examples.spark.apps.apiusage.fixtures

import java.sql.Date

final case class ApiUsageF(
  apiPath: Option[String] = None,
  customer_id: Option[Long] = None,
  date: Option[Date] = None)
