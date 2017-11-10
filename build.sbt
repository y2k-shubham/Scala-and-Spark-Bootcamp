name := "MyProject"
version := "1.0"
scalaVersion := "2.12.4"
libraryDependencies ++= {
  val sparkVer = "2.1.0"
  Seq(
    "org.apache.spark" % "spark-core_2.10" % "1.0.2"
  )
}