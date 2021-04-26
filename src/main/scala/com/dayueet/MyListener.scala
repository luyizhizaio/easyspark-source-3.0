package com.dayueet

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class MyListener extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {

    println("onSunccess funcName:"+funcName)

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
