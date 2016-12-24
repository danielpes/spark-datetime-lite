package danielpes.spark.datetime

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}

package object functions {

  def addPeriod(c: Column, p: Period, dataType: DataType = TimestampType): Column = dataType match {

    case _: DateType => udf(
      (d: java.sql.Date) => DateTimeFunctions.addPeriod(d, p)
    ).apply(c)

    case _: TimestampType => udf(
      (ts: java.sql.Timestamp) => DateTimeFunctions.addPeriod(ts, p)
    ).apply(c)
  }

  def subtractPeriod(c: Column, p: Period, dataType: DataType = TimestampType): Column = {
    addPeriod(c, -p, dataType)
  }
}
