package danielpes.spark.datetime

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, TimestampType}

package object functions {

  def addPeriod(c: Column, p: Period, dataType: DataType = TimestampType): Column = {
    DateTime(c, dataType) + p
  }

  def subPeriod(c: Column, p: Period, dataType: DataType = TimestampType): Column = {
    DateTime(c, dataType) - p
  }
}
