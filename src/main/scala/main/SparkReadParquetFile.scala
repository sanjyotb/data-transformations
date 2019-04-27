package main

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReadParquetFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Hello Spark App").getOrCreate()

    val parquetDF: DataFrame =
      spark
        .read
//        .option("inferSchema", true)
//        .option("header", true)
        .parquet("parquetFile.parquet")

    println(s"First 20 rows : ${parquetDF.show(20, false)}")

    println(s"Schema: ${parquetDF.printSchema()}")

  }

}
