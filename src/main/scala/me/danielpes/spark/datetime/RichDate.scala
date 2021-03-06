package me.danielpes.spark.datetime

import java.util.Calendar

private[datetime] class RichDate[A <: java.util.Date](val datetime: A) {

  private def getCalendar: Calendar = {
    val c = Calendar.getInstance()
    c.setTime(datetime)
    c
  }

  def +(period: Period): Option[A] = {
    Option(datetime) map { _ =>
      val c = getCalendar
      c.add (Calendar.MONTH, period.totalMonths)
      val totalMillis = c.getTimeInMillis + period.totalMilliseconds

      datetime match {
        case _: java.sql.Date => new java.sql.Date (totalMillis).asInstanceOf[A]
        case _: java.sql.Timestamp => new java.sql.Timestamp (totalMillis).asInstanceOf[A]
      }
    }
  }

  def -(p: Period): Option[A] = this + (-p)

  def between[B <: java.util.Date](lower: B, upper: B, includeBounds: Boolean = true): Option[Boolean] = {
    Option(datetime) map { _ =>
      if (includeBounds) datetime.getTime >= lower.getTime && datetime.getTime <= upper.getTime
      else datetime.getTime > lower.getTime && datetime.getTime < upper.getTime
    }
  }

  def toTimestamp: java.sql.Timestamp = new java.sql.Timestamp(datetime.getTime)
  def toDate: java.sql.Date = {
    // We need to reset the time, because java.sql.Date contains time information internally.
    val c = getCalendar
    c.set(Calendar.MILLISECOND, 0)
    c.set(Calendar.SECOND, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.HOUR_OF_DAY, 0)
    new java.sql.Date(c.getTimeInMillis)
  }
}
