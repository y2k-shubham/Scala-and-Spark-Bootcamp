import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val filePath = "/Users/compadmin/Cloud/Code/scala-ide-workspace/Scala-and-Spark-Bootcamp-master/Spark_DataFrames/CitiGroup2006_2008"

// reading dataset
val df_1 = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
df_1.printSchema()

// filtering-1
df_1.filter($"Close" > 480).show()  // scala notation
df_1.filter("Close > 480").show()   // spark notation

