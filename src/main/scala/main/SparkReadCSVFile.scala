package main

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReadCSVFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("Hello Spark App").getOrCreate()

    val csvDF: DataFrame =
      spark
        .read
        .option("inferSchema", true)
        .option("header", true)
        .csv("samplecsv.csv")

    println(s"First 20 rows : ${csvDF.show(20, false)}")

    println(s"Schema: ${csvDF.printSchema()}")

    import org.apache.spark.sql.functions._
    csvDF.select(avg(csvDF("temp_high"))).show()
    csvDF.groupBy().avg("temp_high").show()

  }

}