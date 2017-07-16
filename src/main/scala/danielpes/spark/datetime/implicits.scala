package danielpes.spark.datetime

import org.apache.spark.sql.Column

// Lower-case name to keep the friendly API "import datetime.implicits._"
object implicits {
  implicit class IntImplicits(value: Int) extends RichInt(value)
  implicit class LongImplicits(value: Long) extends RichLong(value)
  implicit class ColumnImplicits(column: Column) extends RichColumn(column)
  implicit class DateImplicits[A <: java.util.Date](date: A) extends RichDate[A](date)
}
