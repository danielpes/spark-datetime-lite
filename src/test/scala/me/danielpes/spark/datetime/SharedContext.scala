package me.danielpes.spark.datetime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.scalatest._

abstract class SharedContext extends FlatSpecLike with BeforeAndAfterAll { self: Suite =>

  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("/executors").setLevel(Level.FATAL)

  private var _spark: SparkSession = _
  protected val debug: Boolean = false

  protected def spark: SparkSession = _spark
  protected def sc: SparkContext = _spark.sparkContext

  protected def assertDFs(ds1: DataFrame, ds2: DataFrame, debug: Boolean = this.debug): Unit = assertDSs[Row](ds1, ds2, debug)

  protected def assertDSs[A](ds1: Dataset[A], ds2: Dataset[A], debug: Boolean = this.debug): Unit = {
    val df1 = ds1.toDF
    val df2 = ds2.toDF
    try {
      df1.persist()
      df2.persist()

      if (debug) {
        df1.printSchema()
        df2.printSchema()
        df1.show(100, truncate=false)
        df2.show(100, truncate=false)
      }
      assert(df1.collect().toSet == df2.collect().toSet)
    } finally {
      df1.unpersist()
      df2.unpersist()
    }
  }

  protected override def beforeAll(): Unit = {
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .appName("Tests")
        .master("local[*]")
        .config("spark.default.parallelism", 2)
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.sql.testkey", "true")
        .getOrCreate()
    }
    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }
}