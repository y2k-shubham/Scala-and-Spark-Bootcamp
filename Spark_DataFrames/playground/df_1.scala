import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val filePath = "/Users/zomadmin/Cloud/Code/scala-ide-workspace/Scala-and-Spark-Bootcamp-master/Spark_DataFrames/CitiGroup2006_2008"

// reading dataset
val df_1 = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

// selecting certain no of rows from dataset
df_1.head(5)

// diplaying dataset line-by-line
println("dataset")
for (row <- df_1.head(5)) {
  println(row)
}

// displaying dataset summary
println("summary")
for (row <- df_1.describe()) {
  println(row)
}

// creating a new column (note the two ways of selecting a column)
val df_2 = df_1.withColumn("High_Plus_Low", (df_1.col("High") + df_1("Low")))
df_2.printSchema()

// selecting and aliasing a column
df_2.select(df_2("High_Plus_Low").as("HPL")).show()

// selecting multiple columns
df_1.select($"High", $"Low")
df_1.select(df_1("High").as("H"), df_1.col("Low"))