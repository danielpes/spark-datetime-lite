package danielpes.spark.datetime

import java.util.Calendar
import scala.collection.immutable.ListMap

case class Period(
    years: Int = 0,
    months: Int = 0,
    days: Int = 0,
    hours: Int = 0,
    minutes: Int = 0,
    seconds: Int = 0,
    milliseconds: Int = 0) {

  def unary_-(): Period = Period.fromList(this.toList.map(-_))

  def +(other: Period): Period = Period.fromList((this.toList, other.toList).zipped.map(_ + _))

  def +[T <: java.util.Date](date: T): T = DateTimeFunctions.addPeriod(date, this)

  def toList: List[Int] = List(years, months, days, hours, minutes, seconds, milliseconds)

  def toMap: ListMap[String, Int] = ListMap(
    "years" -> years,
    "months" -> months,
    "days" -> days,
    "hours" -> hours,
    "minutes" -> minutes,
    "seconds" -> seconds,
    "milliseconds" -> milliseconds
  )

  def getByCalendarUnit: Map[Int, Int] = Map(
    Calendar.YEAR -> years,
    Calendar.MONTH -> months,
    Calendar.DATE -> days,
    Calendar.HOUR -> hours,
    Calendar.MINUTE -> minutes,
    Calendar.SECOND -> seconds,
    Calendar.MILLISECOND -> milliseconds
  )

  def isSingleUnit: Boolean = {
    this.toList.count(_ != 0) == 1
  }

  override def toString: String = {
    val stringList = this.toMap.collect {
      case (k, v) if v == 1 => s"$v ${k.dropRight(1)}"
      case (k, v) if v != 0 => s"$v $k"
    }
    if (stringList.isEmpty) "Empty Period"
    else stringList.mkString(", ")
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[Period]
  override def equals(that: Any): Boolean = that match {
    case that: Period => that.canEqual(this) && this.toList == that.toList
    case _ => false
  }
}

object Period {
  def fromList(list: Seq[Int]) = Period(list(0), list(1), list(2), list(3), list(4), list(5), list(6))
}
