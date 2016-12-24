package danielpes.spark.datetime

import java.sql.{Date, Timestamp}

sealed trait DateTime[T] extends java.util.Date

object DateTime {
  implicit object DateWitness extends DateTime[Date]
  implicit object TimestampWitness extends DateTime[Timestamp]
}
