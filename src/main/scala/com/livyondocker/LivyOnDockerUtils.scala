package com.livyondocker

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LivyOnDockerUtils {

  def getSparkSession(appName: String): SparkSession = {

    val conf = new SparkConf

    conf.set("spark.master", System.getenv("SPARK_MASTER_URL"))
    conf.set("spark.driver.host", "local[*]")
    conf.set("spark.submit.deployMode", "cluster")
    conf.set("spark.app.name", appName)
    conf.set("spark.driver.bindAddress", "0.0.0.0")

    SparkSession.builder.config(conf=conf).getOrCreate()
  }
}
