package com.dayueet

import org.apache.spark.sql.SparkSession

object SparkListenerTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()


    spark.createDataFrame(Seq(("a","b"),("b","c"))).show(false)




    //spark.sparkContext.addSparkListener()

    //spark.listenerManager.register()

  }

}
