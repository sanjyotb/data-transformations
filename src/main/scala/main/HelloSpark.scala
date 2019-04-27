package main

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Hello Spark App").getOrCreate()
    //println("Hello Spark")

    import spark.implicits._
    //val df = Seq(1,2,3).toDF()
    val df = spark.sparkContext.parallelize(Seq(1,2,3)).toDF()

    println(df.count())
    println(df.count())

    Thread.sleep(1000000)
    spark.stop()

  }

}
