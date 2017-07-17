logLevel := Level.Warn
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "latest.integration")

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")
