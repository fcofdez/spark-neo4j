name := "spark-neo4j"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.0",
                            "org.apache.spark" %% "spark-sql" % "1.6.0",
                            "com.databricks" % "spark-csv_2.10" % "1.3.0")
