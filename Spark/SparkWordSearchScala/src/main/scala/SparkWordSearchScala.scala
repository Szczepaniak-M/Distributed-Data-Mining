package de.tum.ddm

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkWordSearchScala {

  private val logger: Logger = Logger.getLogger(classOf[SparkWordSearchScalaMain.type])

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      logger.error("Usage: spark-submit SparkWordSearchScala-1.0.jar <in> <out> <word>")
      System.exit(2)
    }

    val sparkSession = SparkSession.builder
      .appName("SparkWordSearchScala")
//      .master("local[1]") // for local execution
      .getOrCreate

    import sparkSession.implicits._

    val result = sparkSession.read
      .option("multiLine", value = true)
      .json(args(0))
      .drop($"title")
      .select($"id", lower($"text").as("text"))
      .select($"id", regexp_replace($"text", "[^a-z]", " ").as("text"))
      .select($"id", split($"text", " ").as("text"))
      .select($"id", explode($"text").as("word"))
      .where($"word".equalTo(args(2)))
      .drop($"word")
      .distinct()
      .select(count("*"))

    result.write
      .mode("overwrite")
      .csv(args(1))
  }
}
