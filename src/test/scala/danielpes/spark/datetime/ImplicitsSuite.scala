package danielpes.spark.datetime

import org.apache.spark.sql.Column
import org.scalatest.FlatSpec

class ImplicitsSuite extends FlatSpec {

  "this" should "implicitly convert Ints, Longs and Dates" in {
    // Given
    val intVal: Int = 15
    val longVal: Long = 150L
    val dateVal: java.sql.Date = java.sql.Date.valueOf("2010-01-01")
    val columnVal: Column = new Column("a_column")

    // When
    import implicits._
    val richIntVal: RichInt = intVal
    val richLongVal: RichLong = longVal
    val richDateVal: RichDate[java.sql.Date] = dateVal
    val richColumnVal: RichColumn = columnVal

    // Then
    assert(richIntVal.value == intVal)
    assert(richLongVal.value == longVal)
    assert(richDateVal.datetime == dateVal)
    assert(richColumnVal.column == columnVal)
  }
}
