package edu.stanford.cs246
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import math._

object hw2_kmeans_euclidean {

  def stringToFloatArray(line: String) = {
    val myLine = line.split(" ")
    val points = myLine.map(x=>x.toFloat)
    points
  }

  def euclideanDistance(myVector: Array[Float], centroidPoints: RDD[(Array[Float], Long)])={
    // Append all the possible clusters to a point
    val twoVectors = centroidPoints.map(x=>(myVector, x._1, x._2))
    // Take the distance
    val distance = twoVectors.map(x => (x._1,
      x._2,
      sqrt((x._1 zip x._2).map{case (x, y) => pow(y - x, 2)}.sum),
      x._3))
    // Take the minimum distance
    val minDistance = distance.sortBy(_._3).take(1).map(x => (x._1, x._2, x._4))

    minDistance(0)
    minDistance(1)
    minDistance(2)
  }

  def meanOfArrays(myVal: scala.Iterable[Array[Float]]) = {
    var myArray: Array[Float] = Array()
    val myArrays = myVal.toArray
    val total = myArrays.length
    for (i <- 0 to myArrays(0).length){
      var count = 0.0
      for (item <- myArrays){
        count = count + item(i)
      }
      myArray :+ count/total
    }
    myArray
  }

  def newCentroids(d:  RDD[(Long, scala.Iterable[Array[Float]])]) = {

    val gcentroid = d.map(x=>meanOfArrays(x._2))

    gcentroid
  }

  def kMeans(dataPoints: RDD[Array[Float]], centroindPoints: RDD[(Array[Float], Long)], iteraction: Int): RDD[(Array[Float], Array[Float], Long)] = {
    if (iteraction == 1) {

      val distanceRdd = dataPoints.map(x=>euclideanDistance(x, centroindPoints))
      distanceRdd
    }
    else {
      val distanceRdd = dataPoints.map(x=>euclideanDistance(x, centroindPoints))
      val myclusters = distanceRdd.map(x=>(x._3, x._1)).groupByKey()
      val centroidsRdd = newCentroids(myclusters).zipWithIndex()

      kMeans(dataPoints, centroidsRdd, iteraction-1)
    }
  }

  def main(args: Array[String]): Unit = {

    val data_input = args(0)
    val centroids_input = args(1)
    val farcentroids_input = args(2)
    val iterMax = 20

  // Init SparkContext
    val spark = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Euclidean k-means"))

    //  read the data and convert the inputs in float arrayas
    val myInputs = spark.textFile(data_input)
    val points = myInputs.map(line=> stringToFloatArray(line))
    points.take(10).foreach(println)

    // read the centroid data and convert in float arrays
    val myCentroids = spark.textFile(centroids_input).zipWithIndex()
    val centroids = myCentroids.map(line => (stringToFloatArray(line._1), line._2))
    myCentroids.take(10).foreach(println)

    val myclusters = kMeans(points, centroids, iterMax)

    myclusters.take(2).foreach(println)
  }
}
