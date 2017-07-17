package me.danielpes.spark.datetime

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, TimestampType}

private[datetime] class RichColumn(val column: Column) {

  def addPeriod(p: Period, dataType: DataType = TimestampType): Column = {
    DateTimeColumn(column, dataType) + p
  }

  def subPeriod(p: Period, dataType: DataType = TimestampType): Column = {
    DateTimeColumn(column, dataType) - p
  }
}
