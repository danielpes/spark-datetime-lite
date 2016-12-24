# Spark-DateTime

A very simple, lightweight, dependency-free library for extending Spark's date and timestamp operations, focused on time periods.

## Usage Examples

```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import java.sql.{Date, Timestamp}

val sc = new SparkContext()
val sqlContext = new SQLContext(sc)

import sqlContext.implicits._

val df = Seq(
    (Timestamp.valueOf("2010-01-01 15:00:00"), Date.valueOf("2010-07-01")),
    (Timestamp.valueOf("2010-01-01 18:00:00"), Date.valueOf("2010-08-01"))
).toDF("timestamp_col", "date_col")

// Using Column implicits:
import danielpes.spark.datetime.implicits._
df.select($"timestamp_col".addPeriod(1.day + 3.hours + 30.mins))
df.select($"timestamp_col".subtractPeriod(1 hour))
df.select($"date_col".addPeriod(1.yr + 6.months + 12.hours))
df.select($"date_col".addPeriod(5 days, DateType))

// Using functions:
import danielpes.spark.datetime.implicits._
import danielpes.spark.datetime.functions._
df.select(addPeriod($"timestamp_col", 1 hour))

```
