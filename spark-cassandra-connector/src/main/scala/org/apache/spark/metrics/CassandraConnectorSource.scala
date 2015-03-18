package org.apache.spark.metrics

import com.codahale.metrics
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.Source

class CassandraConnectorSource extends Source {
  override val sourceName = "cassandra-connector"

  override val metricRegistry = new metrics.MetricRegistry

  val writeByteMeter = metricRegistry.meter("write-byte-meter")
  val writeRowMeter = metricRegistry.meter("write-row-meter")
  val writeBatchTimer = metricRegistry.timer("write-batch-timer")
  val writeBatchWaitTimer = metricRegistry.timer("write-batch-wait-timer")
  val writeTaskTimer = metricRegistry.timer("write-task-timer")
  
  val writeSuccessCounter = metricRegistry.counter("write-success-counter")
  val writeFailureCounter = metricRegistry.counter("write-failure-counter")

  val readByteMeter = metricRegistry.meter("read-byte-meter")
  val readRowMeter = metricRegistry.meter("read-row-meter")
  val readTaskTimer = metricRegistry.timer("read-task-timer")

  CassandraConnectorSource._instance = Some(this)
  CassandraConnectorSource._env = SparkEnv.get
}

object CassandraConnectorSource {
  @volatile
  private var _instance: Option[CassandraConnectorSource] = None
  @volatile
  private var _env: SparkEnv = null

  def instance = {
    // this simple check allows to control whether the CassandraConnectorSource was created for this
    // particular SparkEnv. If not - we do not report it as enabled.
    if (SparkEnv.get != null && SparkEnv.get == _env && !SparkEnv.get.isStopped)
      _instance
    else
      None
  }

}
