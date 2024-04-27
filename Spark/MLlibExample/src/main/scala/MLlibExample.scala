package de.tum.ddm

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{SparkSession, functions}


// Based on https://www.databricks.com/notebooks/gallery/GettingStartedWithSparkMLlib.html
// Dataset: https://archive.ics.uci.edu/dataset/2/adult

object MLlibExample {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .appName("SparkMLlibExampleScala")
      .master("local[1]")
      .getOrCreate

    import sparkSession.implicits._

    sparkSession.sparkContext.setLogLevel("ERROR")

    val schema =
      """`age` DOUBLE,
         `workclass` STRING,
         `fnlwgt` DOUBLE,
         `education` STRING,
         `education_num` DOUBLE,
         `marital_status` STRING,
         `occupation` STRING,
         `relationship` STRING,
         `race` STRING,
         `sex` STRING,
         `capital_gain` DOUBLE,
         `capital_loss` DOUBLE,
         `hours_per_week` DOUBLE,
         `native_country` STRING,
         `income` STRING"""

    val dataset = sparkSession
      .read
      .schema(schema)
      .csv("input/adult/adult.data")

    dataset.printSchema()

    // Split data to training and test datasets
    val split = dataset.randomSplit(Array(0.8, 0.2), seed = 42)
    val trainDF = split(0)
    val testDF = split(1)

    println(trainDF.cache().count())
    println(testDF.count())

    // Perform simple analysis of data
    trainDF.show(10)

    trainDF
      .select("hours_per_week")
      .summary()
      .show()

    trainDF
      .groupBy("education")
      .count()
      .sort($"count".desc)
      .show()

    // Convert categorical variables to numeric
    val categoricalCols = Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "sex")

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(categoricalCols.map(_ + "Index"))

    val encoder = new OneHotEncoder()
      .setInputCols(stringIndexer.getOutputCols)
      .setOutputCols(categoricalCols.map(_ + "OHE"))

    val labelToIndex = new StringIndexer()
      .setInputCol("income")
      .setOutputCol("label")

    stringIndexer
      .fit(trainDF)
      .transform(trainDF)
      .show(10)


    // Combine all feature columns into a single feature vector
    val numericCols = Array("age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week")
    val assemblerInputs = categoricalCols.map(_ + "OHE") ++ numericCols
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    // Define model
    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setRegParam(1.0)

    // Create Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, encoder, labelToIndex, vecAssembler, lr))

    // Define the pipeline model
    val pipelineModel = pipeline.fit(trainDF)

    // Apply the pipeline model to the test dataset
    val predDF = pipelineModel.transform(testDF)

    // Evaluate the model
    val bcEvaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
    println(f"Area under ROC curve: ${bcEvaluator.evaluate(predDF)}")

    val mcEvaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    println(f"Accuracy: ${mcEvaluator.evaluate(predDF)}")

    // Hyperparameter tuning
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.01, 0.5, 2.0))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // Create a 3-fold CrossValidator
    val cv = new CrossValidator().setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(bcEvaluator)
      .setNumFolds(3)
      .setParallelism(4)

    // Run cross validations. This step takes a few minutes and returns the best model found from the cross validation.
    val cvModel = cv.fit(trainDF)

    // Use the model identified by the cross-validation to make predictions on the test dataset
    val cvPredDF = cvModel.transform(testDF)

    // Evaluate the model's performance based on area under the ROC curve and accuracy
    println(f"Area under ROC curve: ${bcEvaluator.evaluate(cvPredDF)}")
    println(f"Accuracy: ${mcEvaluator.evaluate(cvPredDF)}")


    // Show predictions
    cvPredDF.groupBy($"occupation", $"prediction")
      .agg(functions.count("*").as("count"))
      .select($"occupation", $"prediction", $"count")
      .orderBy($"occupation")
      .show()

    cvPredDF.groupBy($"age", $"prediction")
      .agg(functions.count("*").as("count"))
      .select($"age", $"prediction", $"count")
      .orderBy($"age")
      .show()
  }
}