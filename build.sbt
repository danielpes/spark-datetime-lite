name := "spark-datetime"

version := "0.1.0"

scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")

val sparkVersion = "1.6.2"

parallelExecution in Test := false

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion