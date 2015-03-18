package com.datastax.spark.connector.metrics

import com.codahale.metrics.Timer.Context
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.CassandraConnectorSource

trait MetricsUpdater {
  def finish(): Long
}

trait Timer {
  def stopTimer(): Long
}

trait SimpleTimer extends Timer {
  private val startTime = System.nanoTime()

  override def stopTimer(): Long = System.nanoTime() - startTime
}

trait CCSTimer extends Timer {
  def source: CassandraConnectorSource

  val timer: Context

  override def stopTimer(): Long = {
    val t = timer.stop()
    Option(SparkEnv.get).flatMap(env => Option(env.metricsSystem)).foreach(_.report())
    t
  }
}
