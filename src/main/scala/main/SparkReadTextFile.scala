package main

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReadTextFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello Spark App").getOrCreate()

    val textDF: DataFrame =
      spark
        .read
        .text("sampletext.txt")

    println(s"First 20 rows : ${textDF.show(20, false)}")

    println(s"Schema: ${textDF.printSchema()}")

  }

}

