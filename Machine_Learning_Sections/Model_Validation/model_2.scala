
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)


// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Prepare training and test data.
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("../Regression/Clean-USA-Housing.csv")
data.printSchema()

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = (data.select(data("Price").as("label"),
                      $"Avg Area Income",
                      $"Avg Area House Age",
                      $"Avg Area Number of Rooms",
                      $"Avg Area Number of Bedrooms",
                      $"Area Population"))

val assembler = (new VectorAssembler()
                .setInputCols(Array("Avg Area Income",
                                    "Avg Area House Age",
                                    "Avg Area Number of Rooms",
                                    "Avg Area Number of Bedrooms",
                                    "Area Population"))
                .setOutputCol("features"))

val output = assembler.transform(df).select($"label", $"features")
val Array(training, test) = output.select("label", "features").randomSplit(Array(0.7, 0.3), seed=12345)

val lr = new LinearRegression()
val paramGrid = (new ParamGridBuilder()
                .addGrid(lr.regParam, Array(10000, 0.1))
                .build())
val trainValidationSplit = (new TrainValidationSplit()
    .setEstimator(lr)
    .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
    .setEstimatorParamMaps(paramGrid)
    .setTrainRatio(0.8))

val model = trainValidationSplit.fit(training)
model.transform(test).select("features", "label", "prediction").show()