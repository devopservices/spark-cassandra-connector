package com.datastax.spark.connector.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Timer.Context
import com.datastax.spark.connector.writer.{RichStatement, WriteConf}
import com.google.common.cache.LongAdderBuilder.LongAdderWrapper
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.metrics.CassandraConnectorSource
import org.apache.spark.{Logging, SparkEnv, TaskContext}

private[connector] trait OutputMetricsUpdater extends MetricsUpdater {
  def batchFinished(success: Boolean, stmt: RichStatement, submissionTimestamp: Long, executionTimestamp: Long): Unit = {}

  private[metrics] def updateTaskMetrics(success: Boolean, dataLength: Int): Unit = {}

  private[metrics] def updateCodahaleMetrics(success: Boolean, count: Int, dataLength: Int, submissionTimestamp: Long, executionTimestamp: Long): Unit = {}

}

object OutputMetricsUpdater extends Logging {
  def detailedMetricsEnabled =
    SparkEnv.get.conf.getBoolean("spark.cassandra.output.metrics", defaultValue = true)

  def apply(taskContext: TaskContext, writeConf: WriteConf): OutputMetricsUpdater = {
    val source = CassandraConnectorSource.instance

    if (detailedMetricsEnabled || writeConf.throttlingEnabled) {

      if (!detailedMetricsEnabled) {
        logWarning(s"Output metrics updater disabled, but write throughput limiting requested to ${writeConf.throughputMiBPS} MiB/s." +
          s"Enabling output metrics updater, because it is required by throughput limiting.")
      }

      val tm = taskContext.taskMetrics()
      if (tm.outputMetrics.isEmpty || tm.outputMetrics.get.writeMethod != DataWriteMethod.Hadoop)
        tm.outputMetrics = Some(new OutputMetrics(DataWriteMethod.Hadoop))

      if (source.isDefined)
        new CodahaleAndTaskMetricsUpdater(source.get, tm.outputMetrics.get)
      else
        new TaskMetricsUpdater(tm.outputMetrics.get)

    } else {
      if (source.isDefined)
        new CodahaleMetricsUpdater(source.get)
      else
        new DummyOutputMetricsUpdater
    }
  }

  private abstract class BaseOutputMetricsUpdater
    extends OutputMetricsUpdater with Timer {

    override def batchFinished(success: Boolean, stmt: RichStatement, submissionTimestamp: Long, executionTimestamp: Long): Unit = {
      val dataLength = stmt.bytesCount
      updateTaskMetrics(success, dataLength)
      updateCodahaleMetrics(success, stmt.rowsCount, dataLength, submissionTimestamp, executionTimestamp)
    }

    def finish(): Long = stopTimer()
  }

  private trait TaskMetricsSupport extends OutputMetricsUpdater {
    val outputMetrics: OutputMetrics

    val atomicCounter = new LongAdderWrapper
    atomicCounter.add(outputMetrics.bytesWritten)

    override private[metrics] def updateTaskMetrics(success: Boolean, dataLength: Int): Unit = {
      if (success) {
        atomicCounter.add(dataLength)
        outputMetrics.bytesWritten = atomicCounter.longValue()
      }
    }
  }

  private trait CodahaleMetricsSupport extends OutputMetricsUpdater {
    val source: CassandraConnectorSource

    override private[metrics] def updateCodahaleMetrics(success: Boolean, count: Int, dataLength: Int, submissionTimestamp: Long, executionTimestamp: Long): Unit = {
      if (success) {
        val t = System.nanoTime()
        source.writeBatchTimer.update(t - executionTimestamp, TimeUnit.NANOSECONDS)
        source.writeBatchWaitTimer.update(executionTimestamp - submissionTimestamp, TimeUnit.NANOSECONDS)
        source.writeRowMeter.mark(count)
        source.writeByteMeter.mark(dataLength)
        source.writeSuccessCounter.inc()

      } else {
        source.writeFailureCounter.inc()
      }
    }

    val timer: Context = source.writeTaskTimer.time()
  }

  private class DummyOutputMetricsUpdater extends OutputMetricsUpdater with SimpleTimer {
    def finish(): Long = stopTimer()
  }

  private class TaskMetricsUpdater(val outputMetrics: OutputMetrics)
    extends BaseOutputMetricsUpdater with TaskMetricsSupport with SimpleTimer {
  }

  private class CodahaleMetricsUpdater(val source: CassandraConnectorSource)
    extends BaseOutputMetricsUpdater with CodahaleMetricsSupport with CCSTimer

  private class CodahaleAndTaskMetricsUpdater(val source: CassandraConnectorSource, val outputMetrics: OutputMetrics)
    extends BaseOutputMetricsUpdater with TaskMetricsSupport with CodahaleMetricsSupport with CCSTimer {
  }

}