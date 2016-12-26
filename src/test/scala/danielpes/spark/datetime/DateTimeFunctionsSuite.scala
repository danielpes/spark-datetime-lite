package danielpes.spark.datetime

import org.scalatest.FlatSpec

class DateTimeFunctionsSuite extends FlatSpec {

  "addPeriod" should "allow adding a period to a java.sql.Date" in {
    val period = Period(days = 3, hours = 25)
    val date = java.sql.Date.valueOf("2010-01-01")

    val expected = java.sql.Date.valueOf("2010-01-05")
    val result = DateTimeFunctions.addPeriod(date, period)

    assert(result == expected)
  }

  it should "allow adding a period to a java.sql.Timestamp" in {
    val period = Period(days = 3, hours = 25)
    val timestamp = java.sql.Timestamp.valueOf("2010-01-01 15:00:00")

    val expected = java.sql.Timestamp.valueOf("2010-01-05 16:00:00")
    val result = DateTimeFunctions.addPeriod(timestamp, period)

    assert(result == expected)
  }
}
