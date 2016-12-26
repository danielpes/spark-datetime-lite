package danielpes.spark.datetime

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, TimestampType}

private[datetime] class RichColumn(val c: Column) {

  def addPeriod(p: Period, dataType: DataType = TimestampType): Column = {
    DateTime(c, dataType) + p
  }

  def subPeriod(p: Period, dataType: DataType = TimestampType): Column = {
    DateTime(c, dataType) - p
  }
}
