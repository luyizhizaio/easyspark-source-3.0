package com.dayueet.rpc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MainTest {


  def main(args: Array[String]): Unit = {

    //val spark = SparkSession.builder().master("local").appName("mylocal").getOrCreate()

    val conf = new SparkConf().setMaster("local").setAppName("mylocal")
    val sc = SparkContext.getOrCreate(conf)

    sc.textFile("data/mytext")
      .flatMap{line => line.split(" ")}
      .map{word => word ->1}
      .reduceByKey( _ + _)
      .collect()
      .foreach(println)
  }
}
