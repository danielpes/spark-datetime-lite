package me.danielpes.spark.datetime

import scala.collection.immutable.ListMap

class Period(
    val totalMonths: Int = 0,
    val totalMilliseconds: Long = 0) extends Serializable {

  def years: Int = this.toMap.apply("years").toInt
  def months: Int = this.toMap.apply("months").toInt
  def days: Int = this.toMap.apply("days").toInt
  def hours: Int = this.toMap.apply("hours").toInt
  def minutes: Int = this.toMap.apply("minutes").toInt
  def seconds: Int = this.toMap.apply("seconds").toInt
  def milliseconds: Long = this.toMap.apply("milliseconds")

  def unary_-(): Period = new Period(-totalMonths, -totalMilliseconds)

  def +(other: Period): Period = {
    new Period(totalMonths + other.totalMonths, totalMilliseconds + other.totalMilliseconds)
  }

  def +[T <: java.util.Date](date: T): Option[T] = new RichDate(date) + this

  def toMap: ListMap[String, Long] = {

    val msCounts: List[(String, Long)] = List(
      ("days", Period.MS_PER_DAY),
      ("hours", Period.MS_PER_HOUR),
      ("minutes", Period.MS_PER_MIN),
      ("seconds", Period.MS_PER_SEC),
      ("milliseconds", 1L)
    )
    val initialMap = ListMap(
      "years" -> totalMonths / 12L,
      "months" -> totalMonths % 12L
    )
    msCounts.foldLeft((initialMap, totalMilliseconds)) {
      case ((newMap, rest), (unitName, msPerUnit)) =>
        (newMap + (unitName -> rest / msPerUnit), rest % msPerUnit)
    }._1
  }

  def toList: List[Long] = this.toMap.values.toList

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
    case that: Period => that.canEqual(this) &&
      this.totalMonths == that.totalMonths &&
      this.totalMilliseconds == that.totalMilliseconds
    case _ => false
  }
}

object Period {

  private val MS_PER_SEC: Long =  1000L
  private val MS_PER_MIN: Long =  MS_PER_SEC * 60L
  private val MS_PER_HOUR: Long = MS_PER_MIN * 60L
  private val MS_PER_DAY: Long =  MS_PER_HOUR * 24L

  def apply(
    years: Int = 0,
    months: Int = 0,
    days: Int = 0,
    hours: Int = 0,
    minutes: Int = 0,
    seconds: Int = 0,
    milliseconds: Long = 0): Period = {

    val totalMonths = months + (12 * years)
    val totalMilliseconds = milliseconds +
      seconds * MS_PER_SEC +
      minutes * MS_PER_MIN +
      hours   * MS_PER_HOUR +
      days    * MS_PER_DAY

    new Period(totalMonths, totalMilliseconds)
  }

  def fromList(list: List[Long]) = {
    Period(list(0).toInt, list(1).toInt, list(2).toInt, list(3).toInt, list(4).toInt, list(5).toInt,
      list(6))
  }
}
