package de.tum.ddm

import scala.io.Source
import scala.util.Random
import scala.util.Using

// Alternating optimization algorithm for computing k-Medoids
object KMedoids {

  def main(args: Array[String]): Unit = {
    val k = 5 // Number of clusters

    // 1. Read in data (1024 size array)
    val filePath = "samples.csv"
    val data = processCSVFile(filePath)

    // 2. Select k medoids randomly
    val randomClusterInitializations = selectRandomMedoids(data, k)

    // 3. Iterate while the cost decreases
    //   3.1. In each cluster, find a new medoid - the point that minimizes the sum of distances within the cluster
    //   3.2. Reassign each point to the clusters based on the new medoids
    var medoids = randomClusterInitializations
    var previousCost = Double.MaxValue
    var currentCost = computeCost(data, medoids)
    var clusters: Map[Array[Double], List[Array[Double]]] = Map.empty // Map of medoids and cluster points

    var iteration = 0

    // Iterate while the cost decreases
    while (previousCost > currentCost) {

      // Reassign points to the cluster defined by the closest medoid
      clusters = reassignPointsToClusters(data, medoids)

      // Update medoids
      medoids = updateMedoids(clusters)

      previousCost = currentCost
      currentCost = computeCost(data, medoids)

      iteration += 1
    }

    println(s"Iterations: $iteration")
    println(s"Cost: $currentCost")

  }

  // Transform each line in CSV file to Vector
  private def processCSVFile(filePath: String): List[Array[Double]] = {
    Using(Source.fromFile(filePath)) {
      _.getLines
        .toList
        .map {
          _.split(",")
            .map(_.toDouble)
        }
    }.get
  }

  // Select k random points yo get an initial set of medoids
  private def selectRandomMedoids(data: List[Array[Double]], k: Int): List[Array[Double]] = {
    Random.shuffle(data.indices.toList)
      .take(k)
      .map(data(_))
  }

  // Sum of cosine distance between each point and it's closest medoid.
  private def computeCost(data: List[Array[Double]], medoids: List[Array[Double]]): Double = {
    data.foldLeft(0.0) { (totalCost, point) =>
      val closestMedoid = medoids.minBy(medoid => computeCosineDistance(point, medoid))
      totalCost + computeCosineDistance(point, closestMedoid)
    }
  }

  // Assign each point to the closest medoid using cosine distance
  private def reassignPointsToClusters(data: List[Array[Double]], medoids: List[Array[Double]]): Map[Array[Double], List[Array[Double]]] = {
    var clusters = medoids.map(medoid => medoid -> List.empty[Array[Double]]).toMap

    data.foreach { point =>
      val closestMedoid = medoids.minBy(medoid => computeCosineDistance(point, medoid))
      val currentCluster = clusters(closestMedoid)
      clusters = clusters.updated(closestMedoid, point :: currentCluster)
    }

    clusters
  }

  // For each cluster find a new medoid
  private def updateMedoids(clusters: Map[Array[Double], List[Array[Double]]]): List[Array[Double]] = {
    clusters.values
      .map(findPointMinimizingDissimilarities)
      .toList
  }

  // For each point compute the sum of cosine similarities to other points in the cluster
  // Return the point that minimizes cosine distance sum
  private def findPointMinimizingDissimilarities(clusterPoints: List[Array[Double]]): Array[Double] = {
    require(clusterPoints.nonEmpty, "Cluster points list cannot be empty")

    clusterPoints.minBy { point =>
      clusterPoints.map(otherPoint => computeCosineDistance(point, otherPoint))
        .sum
    }
  }

  // Compute cosine distance
  private def computeCosineDistance(vector1: Array[Double], vector2: Array[Double]): Double =
    1.0 - computeCosineSimilarity(vector1, vector2)

  // Compute cosine similarity
  private def computeCosineSimilarity(vector1: Array[Double], vector2: Array[Double]): Double = {
    require(vector1.length == vector2.length && vector1.length == 1024, "Vectors must have size 1024")

    // Calculate dot product
    val dotProduct = vector1.zip(vector2).map { case (v1, v2) => v1 * v2 }.sum

    // Calculate magnitude of vectors
    val magnitude1 = Math.sqrt(vector1.map(x => x * x).sum)
    val magnitude2 = Math.sqrt(vector2.map(x => x * x).sum)

    // Calculate cosine similarity
    if (magnitude1 != 0 && magnitude2 != 0)
      dotProduct / (magnitude1 * magnitude2)
    else
      0.0 // If either vector has zero magnitude, cosine similarity is zero
  }

}
