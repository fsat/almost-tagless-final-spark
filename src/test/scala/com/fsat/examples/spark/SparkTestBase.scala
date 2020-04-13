package com.fsat.examples.spark

import java.nio.file.{ Files, Path }
import java.sql.{ Date, Timestamp }
import java.time.{ LocalDate, ZoneId, ZoneOffset, ZonedDateTime }
import java.util.UUID

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SparkSession

import scala.reflect.io.Directory

object SparkTestBase {
  def withTempDir[T](deleteOnComplete: Boolean = true)(callback: Path => T): T = {
    val prefix = UUID.randomUUID().toString
    val tempDir = Files.createTempDirectory(prefix)
    try {
      callback(tempDir)
    } finally {
      if (deleteOnComplete) {
        new Directory(tempDir.toFile).deleteRecursively()
      }
    }
  }

  object Dates {
    def timestamp(year: Int, month: Int, date: Int): Timestamp =
      new Timestamp(utcDate(year, month, date).toEpochSecond * 1000)

    def sqlDate(year: Int, month: Int, date: Int): Date =
      Date.valueOf(utcDate(year, month, date).toLocalDate)

    def utcDate(year: Int, month: Int, date: Int): ZonedDateTime = {
      ZonedDateTime.of(year, month, date, 0, 0, 0, 0, ZoneId.of("UTC"))
    }
  }
}

trait SparkTestBase extends TestBase with DataFrameSuiteBase {
  implicit def sparkSession: SparkSession = spark
}
