package de.tum.ddm

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.math.sqrt

object SparkPrimeNumbersScala {

  private val logger: Logger = Logger.getLogger(classOf[SparkPrimeNumbersScalaMain.type])

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      logger.error("Usage: spark-submit SparkPrimeNumbersScala-1.0.jar <in> <out> <max_number>")
      System.exit(2)
    }

    val sparkSession = SparkSession.builder
      .appName("SparkPrimeNumbersScala")
//      .master("local[1]") // for local execution
      .getOrCreate

    import sparkSession.implicits._

    val range = Range(3, sqrt(args(2).toInt).toInt + 1, 2)
    val divisors = sieveOfEratosthenes(range)
    val divisorsBroadcast = sparkSession.sparkContext.broadcast(divisors)

    def isPrime(number: Int): Boolean = {
      if (number < 2) {
        false
      } else if (number == 2) {
        true
      } else if ((number & 1) == 0) {
        false
      } else {
        !divisorsBroadcast.value.filter(number > _).exists(number % _ == 0)
      }
    }

    val results = sparkSession
      .read
      .text(args(0))
      .map(_.getString(0).toInt)
      .filter(number => isPrime(number))
      .orderBy($"value".desc)
      .limit(100)

    results.write
      .mode("overwrite")
      .csv(args(1))
  }

  private def sieveOfEratosthenes(range: Range): Seq[Int] = {
    @tailrec
    def sieveOfEratosthenes(range: IndexedSeq[Int], result: Seq[Int], n: Int): Seq[Int] = {
      val head = range.head
      val tail = range.tail
      if (head < n) {
        sieveOfEratosthenes(tail.filter(_ % head != 0), result :+ head, n)
      } else {
        result ++ range
      }
    }

    sieveOfEratosthenes(range, Seq(), sqrt(range.last).toInt + 1)
  }
}
