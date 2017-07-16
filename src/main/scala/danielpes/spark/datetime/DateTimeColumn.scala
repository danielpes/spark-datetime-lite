package danielpes.spark.datetime

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}
import org.apache.spark.sql.functions.{col, udf}

class DateTimeColumn(val col: Column, dataType: DataType = TimestampType) {

  def +(p: Period): Column = dataType match {
    case _: DateType => udf((d: java.sql.Date) => new RichDate(d) + p).apply(col)
    case _: TimestampType => udf((ts: java.sql.Timestamp) => new RichDate(ts) + p).apply(col)
  }

  def -(p: Period): Column = this.+(-p)

  override def toString: String = s"{column: ${col.toString}, type: ${dataType.toString}}"
}

object DateTimeColumn {

  def apply(col: Column, dataType: DataType = TimestampType) = new DateTimeColumn(col, dataType)
  def apply(col: Column, typeString: String) = new DateTimeColumn(col, typeFromString(typeString))
  def apply(cName: String) = new DateTimeColumn(col(cName), TimestampType)
  def apply(cName: String, dataType: DataType) = new DateTimeColumn(col(cName), dataType)
  def apply(cName: String, typeString: String) = new DateTimeColumn(col(cName), typeFromString(typeString))

  private def typeFromString(s: String): DataType = s match {
    case "date" => DateType
    case "timestamp" => TimestampType
  }
}