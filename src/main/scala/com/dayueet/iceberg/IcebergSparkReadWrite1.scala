package com.dayueet.iceberg

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession};

object IcebergSparkReadWrite1 {

  case class Student(id: Int, name: String, age: Int, dt: String)
  case class Student2(id: Int, name: String, age: Int, grade :Int,dt: String)

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

    //1.创建表写数据
    //writeAndCreateTable()
    //2.写数据
    //write2Table
    //3.更新schema
    //updateSchema
    //4.读数据
    //readData()
    //5.写数据
    //write2TableWithNewSchema
    //6.读取快照对应的数据（查看之前版本的的数据）
    //readSnapShots
    //7.查看namespace
//    showCurrentNamespace
    readData()
//    updateByCondition
    deleteData
    readData()
  }

  /**
   * 读取数据
   * @param sparkSession
   */
  def readData(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    //三种方式
    spark.table("hadoop_prod.db.test1").show(false)
//    sparkSession.read.format("iceberg").load("hadoop_prod.db.test1").show()
    // 路径到表就行，不要到具体文件
    //sparkSession.read.format("iceberg").load("data/dw/db/test1").show()

  }

  /**
   * 创建表写数据
   * @param sparkSession
   */
  def writeAndCreateTable() = {
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val data = spark.createDataset[Student](Array(Student(1001, " 张 三 ", 18, "2021-06-28"),
      Student(1002, "李四", 19, "2021-06-29"), Student(1003, "王五", 20, "2021-06-29")))
    data.writeTo("hadoop_prod.db.test2").partitionedBy(col("dt")) //指定dt 为分区列
      .create()
  }

  /**
   * 追加数据
   */
  def write2Table(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val data = spark.createDataset[Student](Array(Student(1001, " 张 三 ", 18, "2021-06-30"),
      Student(1002, "李四", 19, "2021-06-30"), Student(1003, "王五", 20, "2021-06-30")))

    //两种方式
    // 使 用 DataFrameWriterV2 API
    //data.writeTo("hadoop_prod.db.test1").append()
    //覆盖表
    data.write.format("iceberg").mode(SaveMode.Overwrite).save("hadoop_prod.db.test1")
  }

  def updateSchema(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    val addCol ="ALTER TABLE hadoop_prod.db.test1 ADD COLUMNS (grade Int comment 'grade')"
    spark.sql(addCol)
  }

  //append数据时schema变更
  def write2TableWithNewSchema(): Unit ={
    val spark = SparkSession.builder().getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val data = spark.createDataset[Student2](Array(Student2(1001, " 小明 ", 18, 3,"2021-07-01"),
      Student2(1002, "小四", 19, 3,"2021-07-01"), Student2(1003, "小王", 20, 4,"2021-07-01")))

    data.writeTo("hadoop_prod.db.test1").append()
  }

  def readSnapShots() = {
    val spark = SparkSession.builder().getOrCreate()
    //根据查询 hadoop_prod.db.testA.snapshots 快照表可以知道快照时间和快照id
    //根据时间戳读取，必须是时间戳 不能使用格式化后的时间
    spark.read
      .option("as-of-timestamp", "1654226559682") //毫秒时间戳，查询比该值时间更早的快照
      .format("iceberg")
      .load("hadoop_prod.db.test1").show()

    //根据快照 id 查询
    spark.read
      .option("snapshot-id", "3066351709971432222")
      .format("iceberg")
      .load("hadoop_prod.db.test1").show()
  }

  /**
   * 查看当前namespace
   */
  def showCurrentNamespace(): Unit ={
    val spark = SparkSession.builder().getOrCreate()

    val x= "SHOW CURRENT NAMESPACE"

    spark.sql(x).show(false)
  }

  /**
   * 更新数据操作
   */
  def updateByCondition(): Unit ={
    //从spark3.1开始支持update
    val spark = SparkSession.builder().getOrCreate()
    val x= "UPDATE hadoop_prod.db.test1 SET grade = 10 where grade = 3"
    spark.sql(x).show(false)

  }


  def deleteData()={
    //从spark3.1开始支持update
    val spark = SparkSession.builder().getOrCreate()
    val x= "delete from hadoop_prod.db.test1 where grade = 4"
    spark.sql(x).show(false)
  }

  def mergeInto(): Unit ={

  }


}
