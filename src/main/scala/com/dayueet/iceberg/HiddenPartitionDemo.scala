package com.dayueet.iceberg

import com.dayueet.iceberg.IcebergSparkReadWrite1.Student
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp
import java.util.Date

/**
 * 隐式分区案例
 */
object HiddenPartitionDemo {
  case class BgLog(id: String,idtype:String, productName: String,city:String, eventTime:Timestamp)
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

    //创建表
    //createtable
//    write2Table
//    readData
    descTBL
  }

  /**
   * 读取数据
   * @param sparkSession
   */
  def readData(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    //三种方式
    spark.table("hadoop_prod.db.bg_device_event").show(false)

  }

  /**
   * 追加数据
   */
  def write2Table(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    //id: String,idtype:String, productName: String,city:String, eventTime:Long
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val data = spark.createDataset[BgLog](
      Array(BgLog("abcdefg", "imei", "lanqiu", "北京", new Timestamp(new Date().getTime)),
        BgLog("1223", "idfa", "lanqiu", "合肥", new Timestamp(new Date().getTime)),
        BgLog("12345", "imei", "lanqiu", "合肥", new Timestamp(new Date().getTime)),
        BgLog("1223", "idfa", "lanqiu", "上海", new Timestamp(new Date().getTime)),
        BgLog("abcdefg", "imei", "mz", "北京", new Timestamp(new Date().getTime)))
    )
    data.writeTo("hadoop_prod.db.bg_device_event").append()
  }


  def createtable(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
//id: String,idtype:String, productName: String,city:String, eventTime:Timestamp
    val createTblSql=
      """
        |CREATE TABLE hadoop_prod.db.bg_device_event (
        |    id String,
        |    idtype String,
        |    productName string,
        |    city string,
        |    eventTime timestamp)
        |USING iceberg
        |PARTITIONED BY (idtype,days(eventTime),identity(productName),identity(city))""".stripMargin

    spark.sql(createTblSql)
  }

  def descTBL(): Unit ={
    val spark = SparkSession.builder().getOrCreate()

    val sql = "desc hadoop_prod.db.bg_device_event"

    spark.read.format("iceberg").load("hadoop_prod.db.bg_device_event.files").show(false)

    //spark.sql(sql).show(false)

  }
}
