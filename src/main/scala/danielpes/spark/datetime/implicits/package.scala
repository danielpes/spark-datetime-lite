package danielpes.spark.datetime

import org.apache.spark.sql.Column

package object implicits {

  object IntImplicits {
    implicit def toRichInt(value: Int): RichInt = new RichInt(value)
  }

  object ColumnImplicits {
    implicit def toRichColumn(column: Column): RichColumn = new RichColumn(column)
  }
}

