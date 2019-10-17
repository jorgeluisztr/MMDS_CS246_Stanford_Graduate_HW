package edu.stanford.cs246

import org.apache.spark.{SparkConf, SparkContext}

object hw0 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hw0").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val words = lines.flatMap(l => l.split("[^\\w]+"))
    val wordsalpha = words.filter(x => x.matches("[A-Za-z]+") && x.length > 1)
    val wordlower = wordsalpha.map(x => x.toLowerCase)
    val pairs = wordlower.map(w => (w(0), 1))
    val counts = pairs.reduceByKey((n1, n2) => n1 + n2)

    counts.saveAsTextFile(args(1))
    sc.stop

  }
}