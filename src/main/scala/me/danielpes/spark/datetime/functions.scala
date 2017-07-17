package me.danielpes.spark.datetime

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, TimestampType}

// Lower-case name to keep the friendly API "import datetime.functions._"
object functions {

  def addPeriod(c: Column, p: Period, dataType: DataType = TimestampType): Column = {
    DateTimeColumn(c, dataType) + p
  }

  def subPeriod(c: Column, p: Period, dataType: DataType = TimestampType): Column = {
    DateTimeColumn(c, dataType) - p
  }
}
