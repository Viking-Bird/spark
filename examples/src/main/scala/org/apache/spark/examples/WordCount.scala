package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author pengwang
  * @date 2020/07/07
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val conf = new SparkConf().setAppName("wordCount")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val textFile = sc.textFile(inputFile)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _).count()
  }
}