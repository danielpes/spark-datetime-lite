package danielpes.spark.datetime

private[datetime] class RichLong(val value: Long) {

  def milliseconds: Period = Period(milliseconds = value)
  def millisecond: Period = this.milliseconds
  def ms: Period = this.milliseconds
}
