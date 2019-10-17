package edu.stanford.cs246

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object hw1b {

  def itemAndId(ticketRow: Tuple2[String, Long]) ={

    // list of items and their tickets
    val products = ticketRow._1.split(' ').toArray.map(item => (item, ticketRow._2))

    products
  }

  def L1(productTicket: RDD[(String, Long)], sup: Int) = {

    // count the products
    val myCount = productTicket.countByKey()
    // keep just those whose sum is bigger than support
    val firstFilter = myCount.filter(line => line._2 >= sup).keys.toArray
    firstFilter.map(line=>(line, 0.toLong))
  }

  def pair(alistOfPop: Tuple2[String, Long], ticketOfPop: RDD[(String, Long)], sup:Int) = {

    // Product
    val product = alistOfPop._1

    // Filter sales of popular by the relevant product and keep the the ticket
    val getTickets = ticketOfPop
      .filter(x=> x._1==product)
      .map(x => (x._2, 0))
      .distinct()

    val inverseTicketOfPop = ticketOfPop
      .filter(x => x._1 != product)
      .map(_.swap)

    // count the sales of the products sold togethet with the relevant product
    val getProducts = inverseTicketOfPop
      .leftOuterJoin(getTickets)
      .filter(x => x._2._2.nonEmpty)
      .map(x => (x._2._1, x._1))

    //keep just those whose sum is bigger than support
    val countProducts = getProducts
      .countByKey()
      .filter(x => x._2 >= sup)
      .map(x => (product, x._1))

    countProducts

  }

  def main(args: Array[String]) = {

    val input = args(0)
    val support = args(1).toInt
    val output = args(2)

    // Init SparkContext
    val spark = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("A priori recommendation"))

    // read the data and create an index that will be the ticket number
    val file = spark.textFile(input).zipWithIndex()

    // list items and their tickets
    val tickets = file.flatMap(line => itemAndId(line))

    // L1 Filter
    val filterOne = L1(tickets, support)
    val popular = spark.parallelize(filterOne)

    // join the Tickets and L1 list and keep only products from L1 list
    val popularItem = tickets.leftOuterJoin(popular)
      .filter(x=>x._2._2.nonEmpty)
      .map(y=>(y._1.toString, y._2._1.toLong))

    // Pair of products whose count is bigger that support (L2)
    val pairs = spark.parallelize(filterOne.flatMap(x => pair(x, popularItem, support)))

    pairs.saveAsTextFile(output)

  }
}
