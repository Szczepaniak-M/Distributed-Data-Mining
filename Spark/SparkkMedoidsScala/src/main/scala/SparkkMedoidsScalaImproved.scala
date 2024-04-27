package de.tum.ddm

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkkMedoidsScalaImproved {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    val spark = SparkSession.builder()
      .appName("SparkkMedoidsScalaImproved")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", value = true)
      .getOrCreate()

    import spark.implicits._

    if (args.length != 2) {
      System.err.println("Wrong number of arguments: <number_of_clusters> <file>")
    }
    val k = args(0).toInt
    val filePath = args(1)

    spark.sparkContext.setCheckpointDir("/tmp/checkpoint")

    // 1. Read in data (1024 size vectors)
    val csvData = spark.read
      .option("inferSchema", value = true)
      .csv(filePath)

    val vecAssembler = new VectorAssembler()
      .setInputCols(csvData.columns)
      .setOutputCol("features")

    val vectorData = vecAssembler.transform(csvData)
      .select($"features")
      .withColumn("id", monotonically_increasing_id())
      .cache()

    // 2. Select k Medoids randomly
    val randomClusterInitializations = selectRandomMedoids(vectorData, k)

    // 3. Iterate while the cost decreases
    var medoids = randomClusterInitializations
    var previousCost = Double.PositiveInfinity
    var currentCost = Double.MaxValue
    var iteration = 0
    var iterationsCost = Seq.empty[Double]

    val computeCosineSimilarityUdf = udf((v1: Vector, v2: Vector) => computeCosineSimilarity(v1, v2))

    while ((previousCost - currentCost) / currentCost > 0.005) {

      // Reassign points to the cluster defined by the closest medoid
      val newClusters = computeClusters(vectorData, medoids, computeCosineSimilarityUdf)(spark)

      // Update medoids
      val newMedoids = updateMedoids(newClusters, computeCosineSimilarityUdf)(spark).cache().checkpoint()

      // Compute the new cost
      previousCost = currentCost
      currentCost = computeCost(newMedoids)(spark)
      medoids = newMedoids.drop($"cosineDistanceSum")

      iteration += 1
      iterationsCost = iterationsCost :+ currentCost
    }

    // 4. Save output
    val stop = System.nanoTime()

    val time = s"Time: ${(stop - start) / 1_000_000_000} seconds"
    val iterationCounterStr = s"Iterations: $iteration"
    val iterationsStr = iterationsCost.mkString(",")
    val cost = s"Final cost: $currentCost"

    val result = List(time, iterationCounterStr, iterationsStr, cost)
    val resultRdd = spark.sparkContext.parallelize(result, 1)
    val resultPath = s"/output/result-${spark.sparkContext.applicationId}"
    resultRdd.saveAsTextFile(resultPath)

    spark.stop()
  }

  def selectRandomMedoids(data: Dataset[Row], k: Int): Dataset[Row] =
    data.orderBy(rand())
      .limit(k)
      .withColumnRenamed("features", "medoid")
      .withColumnRenamed("id", "medoidId")

  def computeClusters(data: Dataset[Row], medoids: Dataset[Row], computeCosineSimilarityUdf: UserDefinedFunction)(implicit spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    val dataWithCost = data.crossJoin(medoids)
      .withColumn("cosineDistance", lit(1) - computeCosineSimilarityUdf($"features", $"medoid"))

    val featureWindow = Window.partitionBy($"id")
      .orderBy($"cosineDistance".asc)

    dataWithCost.withColumn("rn", row_number().over(featureWindow))
      .where($"rn" === 1)
      .select($"id", $"features", $"medoidId")
  }

  def updateMedoids(cluster: Dataset[Row], computeCosineSimilarityUdf: UserDefinedFunction)(implicit spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    val candidates = cluster.as("df1").join(cluster.as("df2"), $"df1.medoidId" === $"df2.medoidId")
      .select($"df1.features".as("medoid_candidate"), $"df1.medoidId".as("medoidId"), $"df2.features".as("other_point"))
      .withColumn("cosineDistance", lit(1.0) - computeCosineSimilarityUdf($"medoid_candidate", $"other_point"))
      .groupBy($"medoidId", $"medoid_candidate")
      .agg(sum($"cosineDistance").as("cosineDistanceSum"))

    val medoidWindow = Window.partitionBy($"medoidId")
      .orderBy($"cosineDistanceSum".asc)

    candidates.withColumn("rn", row_number().over(medoidWindow))
      .where($"rn" === 1)
      .select( $"medoidId", $"medoid_candidate".as("medoid"), $"cosineDistanceSum")
  }

  def computeCosineSimilarity(vector1: Vector, vector2: Vector): Double = {
    val dotProduct = vector1.dot(vector2)

    val magnitude1 = Vectors.norm(vector1, 2)
    val magnitude2 = Vectors.norm(vector2, 2)

    if (magnitude1 != 0 && magnitude2 != 0)
      dotProduct / (magnitude1 * magnitude2)
    else
      0.0
  }

  def computeCost(data: Dataset[Row])(implicit spark: SparkSession): Double = {
    import spark.implicits._
    data.agg(sum($"cosineDistanceSum")).first().getAs[Double](0)
  }

}