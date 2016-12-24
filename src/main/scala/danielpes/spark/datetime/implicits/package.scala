package danielpes.spark.datetime

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, TimestampType}

package object implicits {

  implicit class RichInt(value: Int) {

    def years: Period = Period.years(value)
    def year: Period = this.years
    def yrs: Period = this.years
    def yr: Period = this.years

    def months: Period = Period.months(value)
    def month: Period = this.months
    def mo: Period = this.months

    def days: Period = Period.days(value)
    def day: Period = this.days

    def hours: Period = Period.hours(value)
    def hour: Period = this.hours
    def hrs: Period = this.hours
    def hs: Period = this.hours

    def minutes: Period = Period.minutes(value)
    def minute: Period = this.minutes
    def mins: Period = this.minutes
    def min: Period = this.minutes

    def seconds: Period = Period.seconds(value)
    def second: Period = Period.seconds(value)
    def secs: Period = this.seconds
    def sec: Period = this.seconds
    def s: Period = this.seconds

    def milliseconds: Period = Period.milliseconds(value)
    def millis: Period = this.milliseconds
    def milli: Period = Period.milliseconds(value)
    def ms: Period = this.milliseconds
  }

  implicit class RichColumn(val c: Column) {

    def addPeriod(p: Period, dataType: DataType = TimestampType): Column = {
      DateTime(c, dataType) + p
    }

    def subPeriod(p: Period, dataType: DataType = TimestampType): Column = {
      DateTime(c, dataType) - p
    }
  }
}

