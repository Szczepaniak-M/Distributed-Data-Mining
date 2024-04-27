package de.tum.ddm

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object SparkWordCountScala {

  private val logger: Logger = Logger.getLogger(classOf[SparkWordCountScalaMain.type])

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      logger.error("Usage: spark-submit SparkWordCountScala-1.0.jar <in> <out>")
      System.exit(2)
    }

    val sparkSession = SparkSession.builder
      .appName("SparkWordCountScala")
//      .master("local[1]")  // for local execution
      .getOrCreate

    import sparkSession.implicits._

    val results = sparkSession
      .read
      .text(args(0))
      .map(_.getString(0))
      .map(_.toLowerCase.replaceAll("[^a-z]", " "))
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .groupBy($"value")
      .count()

    results.write
      .mode("overwrite")
      .csv(args(1))
  }
}
