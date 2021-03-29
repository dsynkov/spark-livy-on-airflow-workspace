package com.livyondocker

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Properties

object LivyOnDockerUtils {

  def getSparkSession(appName: String): SparkSession = {

    val conf = new SparkConf

    conf.set("spark.master", Properties.envOrElse("SPARK_MASTER_URL", "spark://spark-master:7077"))
    conf.set("spark.driver.host", Properties.envOrElse("SPARK_DRIVER_HOST", "local[*]"))
    conf.set("spark.submit.deployMode", "client")
    conf.set("spark.driver.bindAddress", "0.0.0.0")

    conf.set("spark.app.name", appName)

    SparkSession.builder.config(conf = conf).getOrCreate()
  }
}
