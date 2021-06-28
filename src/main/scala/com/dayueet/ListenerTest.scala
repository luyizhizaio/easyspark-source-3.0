package com.dayueet

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.sql.{SaveMode, SparkSession}
 
object ListenerTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set("spark.extraListeners", classOf[BasicJobCounter].getName)
      //.set("spark.extraListeners", classOf[BasicJobCounter].getName)
      .set("spark.executor.heartbeatInterval", "1000ms")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val fpath = "data/1.csv"
    val csvdf = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(fpath)
    csvdf.select("name").show()
    val schema = csvdf.schema
    val df = spark.createDataFrame(csvdf.rdd.repartition(5).setName("xxxxxxxxxxxxxxxxxxxxxxxxx"), schema)
    //val df = spark.createDataFrame(csvdf.rdd.setName("xxxxxxxxxxxxxxxxxxxxxxxxx"), schema)
    df.write.mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .save("data/out")
    spark.stop()
  }
}
 
private class OnlyExeUpdata extends SparkListener {
 
  val map = scala.collection.mutable.Map.empty[Long, Long]
 
  def getAccOut(list: List[AccumulableInfo], name: String): Option[AccumulableInfo] = {
    list match {
      case Nil => None
      case head :: tail => if (head.name.isDefined && head.name.get == name) {
        Some(head)
      } else {
        getAccOut(tail, name)
      }
    }
 
  }
 
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    printx("onExecutorMetricsUpdate")
    val execId = executorMetricsUpdate.execId
    val accu = executorMetricsUpdate.accumUpdates
    println(s"execId  ${execId}")
    for ((taskId, stageId, stageAttemptId, accumUpdates) <- accu) {
      //println(s"""${stageId}\t${accumUpdates.mkString("<==>")}""")
      /*for (acc <- accumUpdates if (acc.name.isDefined && "number of output rows" == acc.name.get)) {
        println(s"""${taskId}\t${stageId}\t${acc}""")
        if (3L == stageId) {
          sum += acc.update.get.asInstanceOf[Long]
        }
      }*/
      println(s"""==${taskId}  ${accumUpdates.mkString("<==>")}==""")
      val acc = getAccOut(accumUpdates.toList, "number of output rows")
      if (3L == stageId && acc.isDefined) {
        println(s"${taskId} ${acc.get.update.get.asInstanceOf[Long]}")
        map += taskId -> acc.get.update.get.asInstanceOf[Long]
      }
    }
    if (map.size > 0) {
      val sum = map.values.reduce((x, y) => x + y)
      println(s"sum $sum")
    }
 
    printx("onExecutorMetricsUpdate")
  }
 
  def printx(label: String): Unit = {
    println(s"=" * 20 + label + s"=" * 20)
  }
}
 
private class BasicJobCounter extends SparkListener {
  var lacc = 0L
  // app job stage executor task
 
  //======================applicatioin=========================
  /**
    *app 开始时跳去的方法
    * 该方法可以获取 appId，appName ，app开始的时间以及 执行程序的用户
    * @param applicationStart
    */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    printx("onApplicationStart")
    println(s"applicationStart.appAttemptId = ${applicationStart.appAttemptId}")
    println(s"applicationStart.appId = ${applicationStart.appId}")
    println(s"applicationStart.appName = ${applicationStart.appName}")
    println(s"applicationStart.driverLogs = ${applicationStart.driverLogs}")
    println(s"applicationStart.sparkUser = ${applicationStart.sparkUser}")
    println(s"applicationStart.time = ${applicationStart.time}")
    printx("onApplicationStart")
  }
 
  /**
    * app结束时调用的方法
    * 可以获取app结束的时间点
    * @param applicationEnd
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    printx("onApplicationEnd")
    println(s"applicationEnd.time  =  ${applicationEnd.time}")
    printx("onApplicationEnd")
  }
 
  //======================applicatioin=========================
  //======================job===============================
  /**
    * job开始时调用的方法
    * 可以获取jobId,以及该job所包含的stage的信息
    * stage 包括如下信息：stageID，stage 中rdd name，stage 的name 和 task的数量
    * @param jobStart
    */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    printx("onJobStart")
    println(s"jobStart.jobId = ${jobStart.jobId}")
    jobStart.stageIds.foreach(a => println(s"stageId  $a"))
    val stageInfos = jobStart.stageInfos
    stageInfos.foreach {
      si =>
        val rddInfos = si.rddInfos
        println(Seq(
          s"stageId  ${si.stageId}",
          si.rddInfos.map(a => a.name).mkString("rddname ", ",", " rddname"),
          s"si.details ${si.details}",
          s"si.name ${si.name}",
          //si.taskMetrics.accumulators().mkString(","),
          si.accumulables.mkString("accu", ",", "accu"),
          s"si.numTasks ${si.numTasks}").mkString("<<<", "__fgf__", ">>>"))
    }
    printx("onJobStart")
  }
 
  /**
    * jobs 结束时候调用
    * 可以获取jobID, jobResult(job 是否成功)
    * @param jobEnd
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    printx("onJobEnd")
    println(s"jobEnd.jobId  ${jobEnd.jobId}")
    println(s"jobEnd.jobResult  ${jobEnd.jobResult}")
    printx("onJobEnd")
  }
 
  //======================job===============================
 
  //======================stage========================
  /**
    * stage 提交时调用的方法
    * 可以获取stage的一些信息
    * @param stageSubmitted
    */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    printx("onStageSubmitted")
    val si = stageSubmitted.stageInfo
    val rddInfos = si.rddInfos
    println(Seq(
      s"stageId  ${si.stageId}",
      si.rddInfos.map(a => a.name).mkString("rddname ", ",", " rddname"),
      s"si.details ${si.details}",
      s"si.name ${si.name}",
      //si.taskMetrics.accumulators().mkString(","),
      si.accumulables.mkString("accu", ",", "accu"),
      s"si.numTasks ${si.numTasks}").mkString("<<<", "__fgf__", ">>>"))
    printx("onStageSubmitted")
  }
 
  /**
    * stage 结束时调用的方法
    * 可以获取stage 的一些信息，比较重要的accumulables，这里包含了一系列的统计信息，
    * 其中就包含了所read or write 的条数，当然了时该stage 的总条数
    * @param stageCompleted
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    printx("onStageCompleted")
    val si = stageCompleted.stageInfo
    println(s"stageId ${stageCompleted.stageInfo.stageId}")
    val rddInfos = si.rddInfos
    println(Seq(
      s"stageId  ${si.stageId}",
      si.rddInfos.map(a => a.name).mkString("rddname ", ",", " rddname"),
      s"si.details ${si.details}",
      s"si.name ${si.name}",
      si.taskMetrics.resultSize,
      si.accumulables.mkString("accu", ",", "accu"),
      s"si.numTasks ${si.numTasks}").mkString("<<<", "__fgf__", ">>>"))
    printx("onStageCompleted")
  }
 
  //======================stage========================
 
 
  //===========================executor========================
  /**
    * 当executor的统计指标更新时调用的方法，
    * 可以获取 executorID，以及executor中的累加器（重点就在此）
    * 累加器中包含了taskID，stageID，以及统计信息（其中包含了读写的条数）
    * @param executorMetricsUpdate
    */
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    printx("onExecutorMetricsUpdate")
    val execId = executorMetricsUpdate.execId
    val accu = executorMetricsUpdate.accumUpdates
    println(s"execId   ${execId}")
    for ((taskId, stageId, stageAttemptId, accumUpdates) <- accu) {
      //println(s"""${stageId}\t${accumUpdates.mkString("<==>")}""")
      for (acc <- accumUpdates if (acc.name.isDefined && "number of output rows" == acc.name.get)) {
        println(s"""${stageId}\t${taskId}\t${acc}""")
      }
    }
    printx("onExecutorMetricsUpdate")
  }
 
  /**
    * 添加executor时调用的方法
    * @param executorAdded
    */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    printx("onExecutorAdded")
    val exeId = executorAdded.executorId
    println(s"exeId  ${exeId}")
    val exeInfo = executorAdded.executorInfo
    println(s"exeInfo.executorHost ${exeInfo.executorHost}")
    println(s"exeInfo.logUrlMap ${exeInfo.logUrlMap}")
    println(s"exeInfo.totalCores ${exeInfo.totalCores}")
    printx("onExecutorAdded")
  }
 
 
  //===========================executor========================
 
  //======================task===========================
  /**
    * task开始时调用的方法
    * @param taskStart
    */
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    printx("onTaskStart")
    val stageAttempId = taskStart.stageAttemptId
    val stageId = taskStart.stageId
    val taskInfo = taskStart.taskInfo
    println(s"stageAttempId ${stageAttempId}")
    println(s"stageId ${stageId}")
    println(s"""taskInfo ${taskInfo.accumulables.mkString("<==>")}""")
    printx("onTaskStart")
  }
 
  /**
    * task 结束时调用的方法
    * @param taskEnd
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    printx("onTaskEnd")
    val reason = taskEnd.reason
    val sid = taskEnd.stageId
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    println(s"taskend reason ${reason}")
    println(s"stageId sid ${sid}")
    println(s"taskInfo ${taskInfo.accumulables.mkString("<==>")}")
    println(s"""taskMetrics ${taskMetrics.resultSize}""")
    printx("onTaskEnd")
  }
 
  //======================task===========================
 
  def printx(label: String): Unit = {
    println(s"=" * 20 + label + s"=" * 20)
  }
}