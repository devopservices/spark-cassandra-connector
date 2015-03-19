package com.datastax.spark.connector.embedded

import akka.actor.ActorSystem
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

trait SparkTemplate {
  /** Obtains the active [[SparkContext]] object. */
  def sc: SparkContext = SparkTemplate.sc

  /** Obtains the [[ActorSystem]] associated with the active [[SparkEnv]]. */
  def actorSystem: ActorSystem = SparkTemplate.actorSystem

  /** Ensures that the currently running [[SparkContext]] uses the provided configuration. If the
    * configurations are different or force is `true` Spark context is stopped and started again with
    * the given configuration. */
  def useSparkConf(conf: SparkConf = SparkTemplate.defaultConf, force: Boolean = false): SparkContext =
    SparkTemplate.useSparkConf(conf, force)
}

object SparkTemplate {

  /** Default configuration for [[SparkContext]]. */
  val defaultConf = new SparkConf(true)
                    .set("spark.cassandra.connection.host", EmbeddedCassandra.cassandraHost.getHostAddress)
                    .set("spark.cleaner.ttl", "3600")
                    .setMaster(sys.env.getOrElse("IT_TEST_SPARK_MASTER", "local[*]"))
                    .setAppName("Test")

  /** Ensures that the currently running [[SparkContext]] uses the provided configuration. If the
    * configurations are different or force is `true` Spark context is stopped and started again with
    * the given configuration. */
  def useSparkConf(conf: SparkConf = SparkTemplate.defaultConf, force: Boolean = false): SparkContext = {
    if (_sc.getConf.getAll.toMap != conf.getAll.toMap || force)
      resetSparkContext(conf)
    _sc
  }

  private def resetSparkContext(conf: SparkConf) = {
    if (_sc != null) {
      _sc.stop()
    }

    System.err.println("Starting SparkContext with the following configuration:\n" + defaultConf.toDebugString)
    _sc = new SparkContext(conf)
    _sc
  }

  private var _sc: SparkContext = resetSparkContext(defaultConf)

  /** Obtains the active [[SparkContext]] object. */
  def sc: SparkContext = _sc

  /** Obtains the [[ActorSystem]] associated with the active [[SparkEnv]]. */
  def actorSystem: ActorSystem = SparkEnv.get.actorSystem

}
