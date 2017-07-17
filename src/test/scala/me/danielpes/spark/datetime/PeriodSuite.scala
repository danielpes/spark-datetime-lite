package me.danielpes.spark.datetime

import org.scalatest.FlatSpec

class PeriodSuite extends FlatSpec {

  "A Period" should "be created directly by its fields" in {
    val input = Period(7, 6, 5, 4, 3, 2, 1)
    assert(input == new Period(90, 446582001L))
    assert(input == Period(months = 90, milliseconds = 446582001L))
    assert(input == Period.apply(7, 6, 5, 4, 3, 2, 1))
    assert(Period() == Period(0, 0, 0, 0, 0, 0, 0))
    assert(input.years == 7)
    assert(input.months == 6)
    assert(input.days == 5)
    assert(input.hours == 4)
    assert(input.minutes == 3)
    assert(input.seconds == 2)
    assert(input.milliseconds == 1)
  }

  "canEqual" should "correctly identify if the equality is possible" in {
    assert(Period().canEqual(Period(1, 2)))
    assert(!Period(1, 2).canEqual(List(1, 2)))
  }

  "equals" should "correctly identify equal and different periods" in {
    val input = Period(1, 1, 1, 1, 1, 1, 2)
    assert(input != List(1L, 1L, 1L, 1L, 1L, 1L, 2L))
    assert(Period() == Period())
    assert(input != Period())
    assert(Period() != input)
    assert(input == Period(1, 1, 1, 1, 1, 1, 2))
    assert(input != Period(1, 1, 1, 1, 1, 1, 1))
    assert(input != Period(2, 1, 1, 1, 1, 1, 1))
  }

  "Plus operator" should "allow adding two period instances" in {
    val input = Period(months = 1)
    val expected = Period(months = 5, hours = 3)
    val result = input + Period(months = 4, hours = 3)
    assert(result == expected)
  }

  it should "allow adding a period to a java.sql.Date" in {
    val period = Period(days = 3, hours = 25)
    val date = java.sql.Date.valueOf("2010-01-01")

    val expected = new RichDate(date) + period
    val result = period + date

    assert(result == expected)
  }

  it should "allow adding a period to a java.sql.Timestamp" in {
    val period = Period(days = 3, hours = 25)
    val timestamp = java.sql.Timestamp.valueOf("2010-01-01 15:00:00")

    val expected = new RichDate(timestamp) + period
    val result = period + timestamp

    assert(result == expected)
  }

  "The negation symbol" should "invert the signal for all fields" in {
    val input = Period(1, 1, -1, -1, 1, 1, 0)
    val expected = Period(-1, -1, 1, 1, -1, -1, 0)
    val result = -input
    assert(result == expected)
  }

  "toList" should "convert the fields into a list" in {
    val period = Period(1, 2, 3, 4, 5, 6, 7)
    val expected = List(1, 2, 3, 4, 5, 6, 7)
    val result = period.toList
    assert(result == expected)
  }

  "toMap" should "convert the fields into an ordered map" in {
    val period = Period(1, 2, 3, 4, 5, 6, 7)
    val expected = scala.collection.immutable.ListMap[String, Int] (
      "years" -> 1,
      "months" -> 2,
      "days" -> 3,
      "hours" -> 4,
      "minutes" -> 5,
      "seconds" -> 6,
      "milliseconds" -> 7
    )
    val result = period.toMap
    assert(result == expected)
  }

  "isSingleUnit" should "return true if only one field is defined" in {
    assert(Period(months = 1).isSingleUnit)
    assert(!Period().isSingleUnit)
    assert(!Period(1, 2).isSingleUnit)
    assert(!Period(1, 2, 3, 4, 5, 6, 7).isSingleUnit)
  }

  "toString" should "convert the period object into a readable string" in {
    assert(Period().toString == "Empty Period")
    assert(Period(months = 3).toString == "3 months")
    assert(Period(months = 3, years = 1).toString == "1 year, 3 months")
  }

  "fromList" should "statically convert a list into a Period" in {
    val input: List[Long] = List(1, 2, 3, 4, 5, 6, 7)
    assert(Period.fromList(input) == Period(1, 2, 3, 4, 5, 6, 7))
  }
}
