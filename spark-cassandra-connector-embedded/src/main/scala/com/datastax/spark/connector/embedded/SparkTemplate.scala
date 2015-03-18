package com.datastax.spark.connector.embedded

import org.apache.spark.{SparkEnv, SparkConf, SparkContext}

trait SparkTemplate {
  val conf = SparkTemplate.conf
  def sc = SparkTemplate.sc

  def resetSparkContext(conf: SparkConf = conf): Unit = {
    if (sc != null) {
      sc.stop()
    }

    SparkTemplate.sc = new SparkContext(conf)
  }

}

object SparkTemplate {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", EmbeddedCassandra.cassandraHost.getHostAddress)
    .set("spark.cleaner.ttl", "3600")
    .set("spark.metrics.conf", this.getClass.getResource("/metrics.properties").getPath)
    .setMaster(sys.env.getOrElse("IT_TEST_SPARK_MASTER", "local[*]"))
    .setAppName(getClass.getSimpleName)

  System.err.println("The Spark configuration is as follows:\n" + conf.toDebugString)

  var sc = new SparkContext(conf)

  def actorSystem = SparkEnv.get.actorSystem

}
