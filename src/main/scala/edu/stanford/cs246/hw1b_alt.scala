package edu.stanford.cs246

import org.apache.spark.{SparkConf, SparkContext}

object hw1b_alt {

  def itemAndId(ticketRow: Tuple2[String, Long]): Array[(String, Long)] = {
      val prodAndId = ticketRow._1.split(" ").map(prod => (prod, ticketRow._2))
      prodAndId
  }

  def countPerItem(item:String, myCount:scala.collection.Map[String, Long]): Long ={
    val myValue = myCount.filter(x =>x._1==item).values.toArray
    val z = myValue match {
      case Array(x) => x
    }
    z
  }

  def countPerPair(item:(String, String), myCount:scala.collection.Map[(String,String), Long]): Long ={
    val myValue = myCount.filter(x => x._1 == item).values.toArray
    val z = myValue match {
      case Array(x) => x
    }
    z
  }

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val support = args(1).toInt
    val output = args(2)

    // Init SparkContext
    val spark = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("A priori recommendation"))

    // read the data and create an index that will be the ticket number
    val file = spark.textFile(input).zipWithIndex()
    file.take(10).foreach(println)

    // for each product in a ticket return pairs (item, ticket) rdd
    val ticket = file.flatMap(x => itemAndId(x))
    ticket.take(10).foreach(println)

    // in how many tickets products appear  Map(item, numberOfTickets)
    val itemCounter = ticket.countByKey()
    itemCounter.take(20).foreach(println)

    // take the popular items
    val popularItem = spark.parallelize(itemCounter.filter(x=>x._2 > support).keys.toArray.map(x=>(x,1.toLong)))
    popularItem.take(10).foreach(println)

    // just popular item with its tickets
    val ticketWithPopular = ticket.leftOuterJoin(popularItem).filter(x=> x._2._2.nonEmpty).map(x=>(x._1, x._2._1))
    ticketWithPopular.take(10).foreach(println)

    // reverse popular
    val reversePopular = ticketWithPopular.map(x=>(x._2, x._1))
    reversePopular.take(10).foreach(println)

    // join reverses to get pairs
    val candidatePairs = reversePopular.leftOuterJoin(reversePopular).map(x=>((x._2._1.toString,x._2._2 match {
      case Some(x) => x.toString
      case None => ""
    }), x._1)).filter(x=>x._1._1 != x._1._2)
    candidatePairs.take(10).foreach(println)

    // popular pairs
    val popularPairs = candidatePairs.countByKey().filter(x=>x._2>support)
    popularPairs.take(10).foreach(println)

    // popular rdd
    val popularRDD = spark.parallelize(popularPairs.keys.toArray.map(x=>(x._1, x._2)))

    // confidence
    val confidencePair= popularRDD.map(x=>((x._1, x._2), (1.0*countPerPair((x._1,x._2), popularPairs))/countPerItem(x._1, itemCounter)))

    confidencePair.take(10).foreach(println)
  }
}
