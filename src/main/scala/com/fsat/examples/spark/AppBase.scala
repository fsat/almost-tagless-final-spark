package com.fsat.examples.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.util.{ Failure, Success, Try }

trait AppBase extends LazyLogging {
  def run(args: Seq[String])(implicit spark: SparkSession): Try[_]

  protected def createSparkSession(): SparkSession = SparkSession.builder().getOrCreate()

  def main(args: Seq[String]): Unit = {
    implicit val sparkSession = createSparkSession()

    run(args) match {
      case Success(_) =>
        // Do nothing - exit with success
        logger.info("Completed")

      case Failure(e) =>
        logger.error(s"Failure to execute application [${this.getClass.getSimpleName}] with input args $args", e)
        sys.exit(-1)
    }
  }
}
