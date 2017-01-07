package danielpes.spark.datetime

import java.util.Calendar

private[datetime] object DateTimeFunctions {

  private final val calendarUnitList: List[Int] = List(
    Calendar.YEAR,
    Calendar.MONTH,
    Calendar.DATE,
    Calendar.HOUR,
    Calendar.MINUTE,
    Calendar.SECOND,
    Calendar.MILLISECOND
  )

  def addPeriod[T <: java.util.Date](datetime: T, period: Period): T = {
    val c = Calendar.getInstance()
    c.setTime(datetime)
    c.add(Calendar.MONTH, period.totalMonths)
    val totalMillis = c.getTimeInMillis + period.totalMilliseconds

    datetime match {
      case _: java.sql.Date => new java.sql.Date(totalMillis).asInstanceOf[T]
      case _: java.sql.Timestamp => new java.sql.Timestamp(totalMillis).asInstanceOf[T]
    }
  }

  def subtractPeriod[T <: java.util.Date](date: T, period: Period): T = addPeriod(date, -period)

}
