package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object PurchaseByCustomer {
  
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      val customer = fields(0).toInt
      val productPurchase = fields(2).toFloat
      // Create a tuple that is our result.
      (customer, productPurchase)
  }
  
   /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "PurchaseByCustomer")   
    
    // Load each line of data into an RDD
    val input = sc.textFile("../customer-orders.csv")
    
    val results = input.map(parseLine).reduceByKey((a, b) => a + b)
    .map(x => (x._2, x._1))
    .sortByKey()
    
     // Print the results
    for (result <- results) {
      val customer = result._2
      val total = result._1
      println(s"customer num: $customer. total: $total")
    }

  }
}