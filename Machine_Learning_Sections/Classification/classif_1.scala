
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

// Optional: Use the following code below to set the Error reporting
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

// create spark session
val spark = SparkSession.builder().getOrCreate()

val data = (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .format("csv")
            .load("titanic.csv"))
data.printSchema()

val logRegData = (data.select(
                    data("Survived").as("label"),
                    $"Pclass",
                    $"Sex",
                    $"Age",
                    $"SibSp",
                    $"Parch",
                    $"Fare",
                    $"Embarked")
                  ).na.drop()

import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors

// converting strings into numerical values
val genderIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex")
val embarkIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkedIndex")

// convert numerical values into one-hot encoding
val genderEncoder = new OneHotEncoder().setInputCol("SexIndex").setOutputCol("SexVector")
val embarkEncoder = new OneHotEncoder().setInputCol("EmbarkedIndex").setOutputCol("EmbarkedVector")

// label, features
val assembler = (new VectorAssembler().setInputCols(
                    Array("Pclass", "SexVector", "Age", "SibSp", "Parch", "Fare", "EmbarkedVector")
                ).setOutputCol("features"))

// build pipeline
import org.apache.spark.ml.Pipeline
val lr = new LogisticRegression()
val pipeline = (new Pipeline().setStages(Array(
                      genderIndexer, embarkIndexer,
                      genderEncoder, embarkEncoder,
                      assembler, lr)))

// split data into training and test samples
val Array(training, test) = logRegData.randomSplit(weights=Array(0.7, 0.3), seed=12345)

// get model by applying pipeline to training data and get results by applying model to test data
val model = pipeline.fit(training)
val result = model.transform(test)

///////////
// Model Evaluation
///////////

import org.apache.spark.mllib.evaluation.MulticlassMetrics

val predictionAndLabels = result.select($"prediction", $"label").as[(Double, Double)].rdd
val metrics = new MulticlassMetrics(predictionAndLabels)

println("Confusion Matrix")
println(metrics.confusionMatrix)