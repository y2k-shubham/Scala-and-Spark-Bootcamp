import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val filePath = "/Users/zomadmin/Cloud/Code/scala-ide-workspace/Scala-and-Spark-Bootcamp-master/Spark_DataFrames/ContainsNull.csv"

// reading dataset
val df_1 = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
df_1.printSchema()
df_1.show()

// dropping rows with null values
// dropping rows less than 1 non-null columns: take only rows having 1 or more non-null values
df_1.na.drop(1).show()
// take only rows having 2 or more non-null values
df_1.na.drop(2).show()
// take only rows having 3 (more not possible here) non-null values
df_1.na.drop(3).show()

// filling missing values
// fills all numeric type columns with 100
df_1.na.fill(100).show()
// fills all string type columns with "missing text"
df_1.na.fill("missing text").show()
// fill only given list of columns with given values
df_1.na.fill("missing name", Array("Name")).na.fill("missing-id", Array("Id")).na.fill(df_1.select(avg("Sales")), Array("Sales")).show()