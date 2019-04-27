package main

import org.apache.spark.sql.{Dataset, SparkSession}

object Test extends App {

  def isPositive(word: String) = {
    word.length % 2 == 0
  }

  def isNegative(word: String) = {
    word.length % 2 != 0
  }

  val spark = SparkSession.builder().master("local").appName("App").getOrCreate()

  import spark.implicits._

  val wordsRDD1: Dataset[String] = spark.sparkContext.textFile("README.md").flatMap(line => line.split("\\W")).toDS()
  // val positiveWordsCount1 = wordsRDD1.filter(isPositive(_)).count()
  // val negativeWordsCount1 = wordsRDD1.filter(isNegative(_)).count()
  val positiveWordsCount2a = wordsRDD1.count()
  val negativeWordsCount2b = wordsRDD1.count()
  println(wordsRDD1.filter(_ => true).explain())

  val textFile = spark.sparkContext.textFile("README.md")
  val wordsRDD2 = textFile.flatMap(line => line.split("\\W")).toDS()
  wordsRDD2.cache()
  //  val positiveWordsCount2 = wordsRDD2.filter(isPositive(_)).count()
  //  val negativeWordsCount2 = wordsRDD2.filter(isNegative(_)).count()
  val positiveWordsCount2c = wordsRDD1.count()
  val negativeWordsCount2d = wordsRDD1.count()
  println(wordsRDD2.filter(_ => true).explain())

  Thread.sleep(500000)

}