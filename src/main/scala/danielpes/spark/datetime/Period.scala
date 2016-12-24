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

    def years(value: Int): Period = this.copy(years = value)
    def months(value: Int): Period = this.copy(months = value)
    def days(value: Int): Period = this.copy(days = value)
    def hours(value: Int): Period = this.copy(hours = value)
    def minutes(value: Int): Period = this.copy(minutes = value)
    def seconds(value: Int): Period = this.copy(seconds = value)
    def millis(value: Int): Period = this.copy(milliseconds = value)

    def toList: List[Int] = List(years, months, days, hours, minutes, seconds, milliseconds)

    def toMap: ListMap[String, Int] = ListMap(
      "years" -> years,
      "months" -> months,
      "days" -> days,
      "hours" -> hours,
      "minutes" -> minutes,
      "seconds" -> seconds,
      "millis" -> milliseconds
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
      this.toMap.collect {
        case (k, v) if v == 1 => s"$v ${k.dropRight(1)}"
        case (k, v) if v != 0 => s"$v $k"
      }.mkString(", ")
    }
  }

  object Period {
    def years(value: Int) = Period(years = value)
    def months(value: Int) = Period(months = value)
    def days(value: Int) = Period(days = value)
    def hours(value: Int) = Period(hours = value)
    def minutes(value: Int) = Period(minutes = value)
    def seconds(value: Int) = Period(seconds = value)
    def milliseconds(value: Int) = Period(milliseconds = value)
    def fromList(list: Seq[Int]) = Period(list(0), list(1), list(2), list(3), list(4), list(5), list(6))
  }
