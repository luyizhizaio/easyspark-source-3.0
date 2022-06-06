package com.dayueet

import org.apache.spark.sql.SparkSession

object SQLtest {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local").getOrCreate()


    val df = spark.createDataFrame(Seq(("a","b"),("b","c")))


  }

}
