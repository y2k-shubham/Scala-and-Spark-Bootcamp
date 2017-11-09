import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val filePath = "/Users/zomadmin/Cloud/Code/scala-ide-workspace/Scala-and-Spark-Bootcamp-master/Spark_DataFrames/Sales.csv"

// reading dataset
val df_1 = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
df_1.printSchema()

// ordery-by
df_1.orderBy($"Sales".desc).show()

// group-by
//df_1.groupBy("Company").count().show()
//df_1.groupBy("Company").mean().show()
//df_1.groupBy("Company").max().show()
df_1.groupBy("Company").sum().show()

// aggregation
//df_1.select(sum("Sales")).show()
df_1.select(sumDistinct("Sales")).show()