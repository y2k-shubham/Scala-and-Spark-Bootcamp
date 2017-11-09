// DATAFRAME PROJECT
// Use the Netflix_2011_2016.csv file to Answer and complete
// the commented tasks below!

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Load the Netflix Stock CSV File, have Spark infer the data types.
val filePath = "/Users/zomadmin/Cloud/Code/scala-ide-workspace/Scala-and-Spark-Bootcamp-master/Spark_DataFrames/Netflix_2011_2016.csv"
val df_1 = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

// What are the column names?
df_1.columns

// What does the Schema look like?
df_1.printSchema()

// Print out the first 5 columns.
df_1.head(5)
df_1.show(5)

// Use describe() to learn about the DataFrame.
df_1.describe().show()

// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.
df_1.withColumn("HV_Ratio", df_1("High") / df_1("Volume")).show()

// What day had the Peak High in Price?
df_1.filter($"High" === df_1.select(max("High")).first().getDouble(0)).select($"Date", $"High").show()
df_1.orderBy($"High".desc).select($"Date", $"High").show(1)

// What is the mean of the Close column?
df_1.select(mean("Close")).show()

// What is the max and min of the Volume column?
df_1.select(max("Volume"), min("Volume")).show()
df_1.orderBy($"Volume".desc).select($"Date", $"Volume").show(1)

// For Scala/Spark $ Syntax
import spark.implicits._

// How many days was the Close lower than $ 600?
df_1.filter($"Close" < 600).count()
df_1.filter("Close < 600").count()

// What percentage of the time was the High greater than $500 ?
df_1.filter($"High" > 500).count() / df_1.count().toDouble * 100

// What is the Pearson correlation between High and Volume?
df_1.select(corr("High", "Volume")).show()

// What is the max High per year?
df_1.withColumn("Year", year(df_1("Date"))).orderBy("Year").groupBy("Year").max().select($"Year", $"max(High)").show()
df_1.withColumn("Year", year(df_1("Date"))).select($"Year", $"High").orderBy("Year").groupBy("Year").max().select($"Year", $"max(High)").show()
df_1.withColumn("Year", year(df_1("Date"))).select($"Year", $"High").groupBy("Year").max().select($"Year", $"max(High)").orderBy("Year").show()

// What is the average Close for each Calender Month?
df_1.withColumn("Month", month(df_1("Date"))).orderBy("Month").groupBy("Month").mean().select($"Month", $"avg(Close)").show()
df_1.withColumn("Month", month(df_1("Date"))).select($"Month", $"Close").orderBy("Month").groupBy("Month").mean().select($"Month", $"avg(Close)").show()
df_1.withColumn("Month", month(df_1("Date"))).select($"Month", $"Close").groupBy("Month").mean().select($"Month", $"avg(Close)").orderBy("Month").show()