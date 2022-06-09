package com.dayueet.iceberg

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 */
object QueryDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      //hadoop_prod 就是catalog的名称
      .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
      //设置数据仓库地址
      .set("spark.sql.catalog.hadoop_prod.warehouse", "data/dw")
      .set("spark.sql.catalog.catalog-name.type", "hadoop")
      .set("spark.sql.catalog.catalog-name.default-namespace", "default")
      //设置为动态分区
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.session.timeZone", "GMT+8")
      //添加sql 扩展
      .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .setMaster("local[*]")
      .setAppName("table_operations")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    selectManifests
  }

  /**
   * 查询表
   */
  def select (): Unit ={

    val spark = SparkSession.builder().getOrCreate()
    //SELECT * FROM prod.db.table -- catalog: prod, namespace: db, table: table
    val sql ="select * from hadoop_prod.db.bg_device_event"

    spark.sql(sql).show
  }

  def selectMetadataFiles(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    //SELECT * FROM prod.db.table -- catalog: prod, namespace: db, table: table
    val sql ="select * from hadoop_prod.db.bg_device_event.files"

    spark.sql(sql).show(50,false)
  }

  def selectSnapshots(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    val sql ="select * from hadoop_prod.db.bg_device_event.snapshots"

    spark.sql(sql).show(50,false)
  }


  def incrementalRead(): Unit ={
    val spark = SparkSession.builder().getOrCreate()

    spark.read.format("iceberg")
      .option("start-snapshot-id", "3586234638228132617")
      .load("data/dw/db/bg_device_event").show
  }


  def selectHistory(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    val sql ="select * from hadoop_prod.db.bg_device_event.history"

    spark.sql(sql).show(50,false)
  }


  def selectManifests(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    val sql ="select * from hadoop_prod.db.bg_device_event.manifests"

    spark.sql(sql).show(50,false)
  }

}
