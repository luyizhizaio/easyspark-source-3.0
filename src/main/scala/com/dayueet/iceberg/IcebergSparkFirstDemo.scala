package com.dayueet.iceberg

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession;

object IcebergSparkFirstDemo {

  case class Student(id: Int, name: String, age: Int, dt: String)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .set("spark.sql.catalog.hadoop_prod.warehouse", "/data/dw")
      .set("spark.sql.catalog.catalog-name.type", "hadoop")
      .set("spark.sql.catalog.catalog-name.default-namespace", "default")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.session.timeZone", "GMT+8")
      .setMaster("local[*]").setAppName("table_operations")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    writeAndCreateTable(spark)

  }

  def writeAndCreateTable(sparkSession: SparkSession) = {
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val data = sparkSession.createDataset[Student](Array(Student(1001, " 张 三 ", 18, "2021-06-28"),
      Student(1002, "李四", 19, "2021-06-29"), Student(1003, "王五", 20, "2021-06-29")))
    data.writeTo("hadoop_prod.db.test1").partitionedBy(col("dt")) //指定dt 为分区列
      .create()
  }



    def writeData(): Unit ={

  }

}
