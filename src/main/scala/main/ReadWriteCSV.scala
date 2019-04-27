package main

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadWriteCSV extends App {

  val spark = SparkSession.builder.master("local").appName("Hello Spark App").getOrCreate()

  val csvDF: DataFrame =
    spark
      .read
      .option("inferSchema", true)
      .option("header", true)
      .csv("samplecsv.csv")

  println(s"First 20 rows : ${csvDF.show(20, false)}")

  println(s"Schema: ${csvDF.printSchema()}")

  /*csvDF
    .write
    .option("inferSchema", true)
    .option("header", true)
    .csv("NewSampleCSV.csv")*/

}
