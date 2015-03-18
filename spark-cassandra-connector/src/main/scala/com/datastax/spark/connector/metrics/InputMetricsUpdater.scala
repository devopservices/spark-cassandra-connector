package com.datastax.spark.connector.metrics

import com.datastax.driver.core.Row
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.TaskContext
import org.apache.spark.executor.{DataReadMethod, InputMetrics}
import org.apache.spark.metrics.CassandraConnectorSource

private[connector] trait InputMetricsUpdater extends MetricsUpdater {
  /**
   * This methods is not thread-safe.
   */
  def updateMetrics(row: Row): Row = row

  private[metrics] def updateTaskMetrics(dataLength: Int): Unit = {}

  private[metrics] def updateCodahaleMetrics(count: Int, dataLength: Int): Unit = {}

}

object InputMetricsUpdater {
  val DefaultGroupSize = 100

  def apply(taskContext: TaskContext, readConf: ReadConf, groupSize: Int = DefaultGroupSize): InputMetricsUpdater = {
    val source = CassandraConnectorSource.instance

    if (readConf.taskMetricsEnabled) {
      val tm = taskContext.taskMetrics()
      if (tm.inputMetrics.isEmpty || tm.inputMetrics.get.readMethod != DataReadMethod.Hadoop)
        tm.inputMetrics = Some(new InputMetrics(DataReadMethod.Hadoop))

      if (source.isDefined)
        new CodahaleAndTaskMetricsUpdater(groupSize, source.get, tm.inputMetrics.get)
      else
        new TaskMetricsUpdater(groupSize, tm.inputMetrics.get)

    } else {
      if (source.isDefined)
        new CodahaleMetricsUpdater(groupSize, source.get)
      else
        new DummyInputMetricsUpdater()
    }
  }

  private abstract class CumulativeInputMetricsUpdater(groupSize: Int)
    extends InputMetricsUpdater with Timer {
    require(groupSize > 0)

    private var cnt = 0
    private var dataLength = 0

    override def updateMetrics(row: Row): Row = {
      var rowLength = 0
      for (i <- 0 until row.getColumnDefinitions.size() if !row.isNull(i))
        rowLength += row.getBytesUnsafe(i).remaining()

      // updating task metrics is cheap
      updateTaskMetrics(rowLength)

      cnt += 1
      dataLength += rowLength
      if (cnt == groupSize) {
        // this is not that cheap because Codahale metrics are thread-safe
        updateCodahaleMetrics(cnt, dataLength)
        cnt = 0
        dataLength = 0
      }
      row
    }

    def finish(): Long = {
      updateCodahaleMetrics(cnt, dataLength)
      val t = stopTimer()
      t
    }
  }

  private class DummyInputMetricsUpdater extends InputMetricsUpdater with SimpleTimer {
    def finish(): Long = stopTimer()
  }

  private trait CodahaleMetricsSupport extends InputMetricsUpdater {
    val source: CassandraConnectorSource

    @inline
    override def updateCodahaleMetrics(count: Int, dataLength: Int): Unit = {
      source.readByteMeter.mark(dataLength)
      source.readRowMeter.mark(count)
    }

    val timer = source.readTaskTimer.time()
  }

  private trait TaskMetricsSupport extends InputMetricsUpdater {
    val inputMetrics: InputMetrics

    @inline
    override def updateTaskMetrics(dataLength: Int): Unit = inputMetrics.bytesRead += dataLength
  }

  private class TaskMetricsUpdater(groupSize: Int, val inputMetrics: InputMetrics)
    extends CumulativeInputMetricsUpdater(groupSize) with TaskMetricsSupport with SimpleTimer {
  }

  private class CodahaleMetricsUpdater(groupSize: Int, val source: CassandraConnectorSource)
    extends CumulativeInputMetricsUpdater(groupSize) with CodahaleMetricsSupport with CCSTimer

  private class CodahaleAndTaskMetricsUpdater(groupSize: Int, val source: CassandraConnectorSource, val inputMetrics: InputMetrics)
    extends CumulativeInputMetricsUpdater(groupSize) with TaskMetricsSupport with CodahaleMetricsSupport with CCSTimer {
  }

}