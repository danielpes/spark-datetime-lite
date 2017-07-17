name := "spark-datetime-lite"
spName := "danielpes/spark-datetime-lite"

version := "0.1.0"

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")
spAppendScalaVersion := true

sparkVersion := "2.2.0"
sparkComponents += "sql"

parallelExecution in Test := false

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"
