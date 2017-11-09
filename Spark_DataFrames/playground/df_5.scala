import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val filePath = "/Users/zomadmin/Cloud/Code/scala-ide-workspace/Scala-and-Spark-Bootcamp-master/Spark_DataFrames/CitiGroup2006_2008"

// reading dataset
val df_1 = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
df_1.printSchema()

// get month / year (number) of all dates
//df_1.select(month(df_1("Date"))).show()
//df_1.select(year(df_1("Date"))).show()

// average price for every  year
val df_2 = df_1.withColumn("year", year(df_1("Date")))
val df_3 = df_2.groupBy("year").mean().orderBy("Year")
df_3.select($"Year", $"avg(Close)").show()

// min price for every year
val df_4 = df_1.withColumn("year", year(df_1("Date")))
val df_5 = df_4.groupBy("year").min().orderBy("Year")
df_5.select($"Year", $"min(Close)").show()
