import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.log4j._

Logger.getLogger("org").setLevel(Level.ERROR)
val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("../Regression/Clean-USA-Housing.csv")
data.printSchema

println("data")
data.show()

// See an example of what the data looks like
// by printing out a Row
//val colnames = data.columns
//val firstrow = data.head(1)(0)
//println("\n")
//println("Example Data Row")
//for(ind <- Range(1,colnames.length)){
//  println(colnames(ind))
//  println(firstrow(ind))
//  println("\n")
//}

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = (data.select(data("Price").as("label"),
                      $"Avg Area Income",
                      $"Avg Area House Age",
                      $"Avg Area Number of Rooms",
                      $"Area Population"))
val assembler = (new VectorAssembler()
                    .setInputCols(Array(
                      "Avg Area Income",
                      "Avg Area House Age",
                      "Avg Area Number of Rooms",
                      "Area Population"))
                    .setOutputCol("features"))
val output = assembler.transform(df).select($"label", $"features")
//output.show()

val lr = new LinearRegression()
val lrModel = lr.fit(output)
val trainingSummary = lrModel.summary

println("training summary")
trainingSummary.predictions.show()

println("residuals = (prediction - label)")
trainingSummary.residuals.show()

println("variance")
trainingSummary.r2