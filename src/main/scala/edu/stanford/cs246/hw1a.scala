package edu.stanford.cs246

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object hw1a {


  def friends(personAndFriendsList: String): Array[((Int, Int), Int)] = {
    val input = personAndFriendsList.split('\t')

    // If the person has no id or if does not have any friends do nothing
    if (input(0) == "" || input.length == 1)
      return Array.empty[((Int, Int), Int)]


    val person = input(0).toInt
    val friendsList = input(1)

    // We will start considering all possible friendships
    friendsList.split(",")
      .map(friend => ((person, friend.toInt), 0))}

  def potentialMutualFriendships(user_friends_line: String): Array[((Int, Int), Int)]  = {
    val input = user_friends_line.split('\t')

    // If the person has no id or if does not have any friends do nothing
    if (input(0) == "" || input.length == 1)
      return Array.empty[((Int, Int), Int)]

    val friendsList = input(1).split(",").map(friend => friend.toInt)

    // Create a list of potential mutual friendships
    var potentialFriendships = ListBuffer.empty[((Int, Int), Int)]

    for {
      i <- 0 until friendsList.length
      j <- 0 until friendsList.length
      if i != j
    } potentialFriendships.append(((friendsList(i), friendsList(j)), 1))

    potentialFriendships.toArray
  }

  def recommend_new_friends(people: List[(Int, Int)], numOfRecomm: Int) : List[Int]   = {
    people.sortBy(person_mutualfriendscount => (-person_mutualfriendscount._2, person_mutualfriendscount._1))
      .take(numOfRecomm)
      .map(person_mutualfriendscount=> person_mutualfriendscount._1)
  }

  def main(args: Array[String]): Unit = {

    val input = args(0)
    val numberOfRecommendations = args(1).toInt
    val output = args(2)

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Friend Recommender"))
    val file = sc.textFile(input)
    val realFriends = file.flatMap(line => friends(line))
    val possibleMutualFriends = file.flatMap(line => potentialMutualFriendships(line))

    // Now we will join the possible mutual friends with the actual friends
    val mutualFriends = possibleMutualFriends.leftOuterJoin(realFriends)
      .filter(pair => pair._2._2.isEmpty)
      .map(pair => ((pair._1._1, pair._1._2), pair._2._1))

    val friendRecommendations = mutualFriends
      .reduceByKey((a, b) => a + b)
      .map(two_with_mutualfriends => (two_with_mutualfriends._1._1, (two_with_mutualfriends._1._2, two_with_mutualfriends._2)))
      .groupByKey()
      .map(tup2 => (tup2._1, recommend_new_friends(tup2._2.toList, numberOfRecommendations)))
      .map(tup2 => tup2._1.toString + "\t" + tup2._2.map(x=>x.toString).toArray.mkString(","))

    friendRecommendations.saveAsTextFile(output)
    // Be assure of your Results
    /*
    friendRecommendations
      .map(line => line.split('\t'))
      .filter(person => person(0).toInt == 11).foreach(println)
    */
  }
}