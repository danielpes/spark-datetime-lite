package danielpes.spark.datetime

private[datetime] class RichInt(val underlying: Int) extends AnyVal {

  def years: Period = Period(years = underlying)
  def year: Period = this.years
  def yrs: Period = this.years
  def yr: Period = this.years

  def months: Period = Period(months = underlying)
  def month: Period = this.months
  def mo: Period = this.months

  def days: Period = Period(days = underlying)
  def day: Period = this.days

  def hours: Period = Period(hours = underlying)
  def hour: Period = this.hours
  def hrs: Period = this.hours
  def hs: Period = this.hours

  def minutes: Period = Period(minutes = underlying)
  def minute: Period = this.minutes
  def mins: Period = this.minutes
  def min: Period = this.minutes

  def seconds: Period = Period(seconds = underlying)
  def second: Period = this.seconds
  def secs: Period = this.seconds
  def sec: Period = this.seconds
  def s: Period = this.seconds

  def milliseconds: Period = Period(milliseconds = underlying)
  def millis: Period = this.milliseconds
  def milli: Period = this.milliseconds
  def ms: Period = this.milliseconds
}
