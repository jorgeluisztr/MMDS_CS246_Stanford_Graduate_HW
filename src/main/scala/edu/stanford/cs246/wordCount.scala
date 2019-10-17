package edu.stanford.cs246

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.SparkContext._

object wordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val words = lines.flatMap(l => l.split("[^\\w]+"))
    val pairs = words.map(w => (w, 1))
    val counts = pairs.reduceByKey((n1, n2) => n1 + n2)

    counts.saveAsTextFile(args(1))
    sc.stop
  }
}