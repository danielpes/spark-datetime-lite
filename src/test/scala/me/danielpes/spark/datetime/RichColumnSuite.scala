package me.danielpes.spark.datetime

import org.apache.spark.sql.types.DateType

class RichColumnSuite extends SharedContext {

  "addPeriod" should "correctly add a period to a date column" in {

    val spark = super.spark
    import spark.implicits._

    // Given
    val period = Period(days = 3)
    val input = Seq(
      Tuple1(Some(java.sql.Date.valueOf("2010-01-01"))),
      Tuple1(None)
    ).toDF("date_col")
    val expected = Seq(
      Tuple1(Some(java.sql.Date.valueOf("2010-01-04"))),
      Tuple1(None)
    ).toDF("date_col")

    // When
    val result = input.select(new RichColumn($"date_col").addPeriod(period, DateType))

    // Then
    assertDFs(result, expected, debug=true)
  }

  it should "correctly add a period to a timestamp column" in {

    val spark = super.spark
    import spark.implicits._

    // Given
    val period = Period(days = 3, hours = 25)
    val input = Seq(
      Tuple1(Some(java.sql.Timestamp.valueOf("2010-01-01 15:00:00"))),
      Tuple1(None)
    ).toDF("timestamp_col")
    val expected = Seq(
      Tuple1(Some(java.sql.Timestamp.valueOf("2010-01-05 16:00:00"))),
      Tuple1(None)
    ).toDF("timestamp_col")

    // When
    val result = input.select(new RichColumn($"timestamp_col").addPeriod(period))

    // Then
    assertDFs(result, expected, debug=true)
  }

  "subPeriod" should "correctly substract a period from a date column" in {

    val spark = super.spark
    import spark.implicits._

    // Given
    val period = Period(days = 3)
    val input = Seq(
      Tuple1(Some(java.sql.Date.valueOf("2010-01-01"))),
      Tuple1(None)
    ).toDF("date_col")
    val expected = Seq(
      Tuple1(Some(java.sql.Date.valueOf("2009-12-29"))),
      Tuple1(None)
    ).toDF("date_col")

    // When
    val result = input.select(new RichColumn($"date_col").subPeriod(period, DateType))

    // Then
    assertDFs(result, expected, debug=true)
  }

  it should "correctly subtract a period from a timestamp column" in {

    val spark = super.spark
    import spark.implicits._

    // Given
    val period = Period(days = 3, hours = 25)
    val input = Seq(
      Tuple1(Some(java.sql.Timestamp.valueOf("2010-01-01 15:00:00"))),
      Tuple1(None)
    ).toDF("timestamp_col")
    val expected = Seq(
      Tuple1(Some(java.sql.Timestamp.valueOf("2009-12-28 14:00:00"))),
      Tuple1(None)
    ).toDF("timestamp_col")

    // When
    val result = input.select(new RichColumn($"timestamp_col").subPeriod(period))

    // Then
    assertDFs(result, expected, debug=true)
  }
}
