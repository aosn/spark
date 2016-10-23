package practice.ch06

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object quicksample {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()

    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word =>(word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print

    ssc.start
  }
}
