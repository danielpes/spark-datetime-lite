package danielpes.spark.datetime

import java.sql.Date
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

package object functions {
  def addInterval(c: Column, i: Period): Column = {
    udf((date: Date) => date).apply(c)
  }
}
