package de.tum.ddm

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object SparkkMedoidsScalaSampling {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    val spark = SparkSession.builder()
      .appName("SparkkMedoidsScalaSampling")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("/tmp/checkpoint")

    import spark.implicits._

    if (args.length != 2) {
      System.err.println("Wrong number of arguments: <number_of_clusters> <file>")
    }
    val k = args(0).toInt
    val filePath = args(1)

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
    val randomMedoids = selectRandomMedoids(vectorData, k)

    // 3. Iterate while the cost decreases
    var medoids = randomMedoids
    var previousCost = Double.PositiveInfinity
    var currentCost = Double.MaxValue
    var iteration = 0
    var iterationsCost = Seq.empty[Double]

    val computeCosineSimilarityUdf = udf((v1: Vector, v2: Vector) => computeCosineSimilarity(v1, v2))

    // Initial random assigment
    var clusterAssigment = assignRandomMedoid(vectorData, randomMedoids, k, computeCosineSimilarityUdf)(spark).cache()

    while (previousCost > currentCost && (previousCost - currentCost) / currentCost > 0.005) {

      // Sample data
      val randomSample = clusterAssigment.sample(0.2)
        .select($"id", $"features")

      // Reassign points to the cluster defined by the closest medoid
      val sampleClusterAssigment = computeClusters(randomSample, medoids, computeCosineSimilarityUdf)(spark)
      clusterAssigment = updateClusterAssigment(clusterAssigment, sampleClusterAssigment)(spark).cache().checkpoint()

      // Update medoids
      medoids = updateMedoids(clusterAssigment.sample(0.2), computeCosineSimilarityUdf)(spark)

      // Compute the new cost
      previousCost = currentCost
      currentCost = computeCost(clusterAssigment)(spark)

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

  def assignRandomMedoid(data: Dataset[Row], medoids: Dataset[Row], k: Int, computeCosineSimilarityUdf: UserDefinedFunction)(implicit spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    val fraction = lit(1.0 / k)

    val dataWithRandId = data.withColumn("rand", rand())
      .withColumn("randId", floor($"rand" / fraction))

    val randIdWindow = Window.orderBy($"medoidId")
    val medoidsWithRandId = medoids.withColumn("randId", row_number.over(randIdWindow) - lit(1))

    dataWithRandId.as("df1").join(medoidsWithRandId.as("df2"), $"df1.randId" === $"df2.randId")
      .select($"id", $"features", $"medoidId", $"medoid")
      .withColumn("cosineDistance", lit(1) - computeCosineSimilarityUdf($"features", $"medoid"))
  }

  def computeClusters(data: Dataset[Row], medoids: Dataset[Row], computeCosineSimilarityUdf: UserDefinedFunction)(implicit spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    val dataWithCost = data.crossJoin(medoids)
      .withColumn("cosineDistance", lit(1) - computeCosineSimilarityUdf($"features", $"medoid"))

    val featureWindow = Window.partitionBy($"id")
      .orderBy($"cosineDistance".asc)

    dataWithCost.withColumn("rn", row_number().over(featureWindow))
      .where($"rn" === 1)
      .drop($"rn")
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
      .select($"medoidId", $"medoid_candidate".as("medoid"))
  }

  def updateClusterAssigment(clusterAssigment: Dataset[Row], newClusterAssigment: Dataset[Row])(implicit spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    val clusterAssigmentWithoutNew = clusterAssigment.as("df1").join(newClusterAssigment.as("df2"), $"df1.id" === $"df2.id", "left_anti")
      .select($"id", $"features", $"medoidId", $"medoid", $"cosineDistance")

    val newClusterAssigmentReorderCols = newClusterAssigment.select($"id", $"features", $"medoidId", $"medoid", $"cosineDistance")

    clusterAssigmentWithoutNew.union(newClusterAssigmentReorderCols)
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
    data.agg(sum($"cosineDistance")).first().getAs[Double](0)
  }

}