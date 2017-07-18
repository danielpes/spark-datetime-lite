[![Build Status](https://travis-ci.org/danielpes/spark-datetime-lite.svg?branch=master)](https://travis-ci.org/danielpes/spark-datetime-lite)
[![codecov](https://codecov.io/gh/danielpes/spark-datetime-lite/branch/master/graph/badge.svg)](https://codecov.io/gh/danielpes/spark-datetime-lite)

# Spark DateTime Lite

A lightweight, dependency-free package for extending Spark's date and timestamp operations, focused on time periods.

This package uses only java.util.Calendar, java.util.Date, java.sql.Date and java.sql.Timestamp for the date operations.

## Adding to your project

This library is available in [Spark Packages](https://spark-packages.org/package/danielpes/spark-datetime-lite), so you can choose one of the following strategies to add it to your project.

### spark-shell, pyspark, or spark-submit
Just pass it using the `--packages` parameter. For example:
```
$SPARK_HOME/bin/spark-shell --packages danielpes:spark-datetime-lite:0.1.0-s_2.11
```

### SBT

If you use the sbt-spark-package plugin, in your sbt build file, add:
```
spDependencies += "danielpes/spark-datetime-lite:0.1.0-s_2.11"
```

Otherwise,
```
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "danielpes" % "spark-datetime-lite" % "0.1.0-s_2.11"
```

### Maven
In your pom.xml, add:
```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>danielpes</groupId>
    <artifactId>spark-datetime-lite</artifactId>
    <version>0.1.0-s_2.11</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```

## Usage Examples

_Example context and input data:_

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import java.sql.{Date, Timestamp}

val spark = SparkSession.builder().getOrCreate()
import spark.implicits._

val df = Seq(
  (Timestamp.valueOf("2010-01-01 15:00:00"), Date.valueOf("2010-07-01")),
  (Timestamp.valueOf("2010-01-01 18:00:00"), Date.valueOf("2010-08-01"))
).toDF("timestamp_col", "date_col")
```

**Using Dataset and map:**
```scala
case class MyClass(timestamp_col: Timestamp, date_col: Date)
val ds = df.as[MyClass]

import me.danielpes.spark.datetime.implicits._
ds.map {
  case MyClass(ts: Timestamp, date: Date) => MyClass(ts + 30.minutes, date + 5.days)
}
```

**Using Column implicits:**
```scala
import me.danielpes.spark.datetime.implicits._
df.select($"timestamp_col".addPeriod(1.day + 3.hours + 30.mins))
df.select($"timestamp_col".subPeriod(1 hour))
df.select($"date_col".addPeriod(1.yr + 6.months + 12.hours))
df.select($"date_col".addPeriod(5 days, DateType))
```

**Using Column functions:**
```scala
import me.danielpes.spark.datetime.implicits._
import me.danielpes.spark.datetime.functions._
df.select(addPeriod($"timestamp_col", 1.hour))
```

**Using the DateTime wrapper class:**
```scala
import me.danielpes.spark.datetime.implicits._
import me.danielpes.spark.datetime.{DateTimeColumn => dt}
df.select(dt("timestamp_col") + 1.hour)
df.select(dt($"timestamp_col") + 1.year)
df.select(dt("date_col", DateType) + 3.days)
```
