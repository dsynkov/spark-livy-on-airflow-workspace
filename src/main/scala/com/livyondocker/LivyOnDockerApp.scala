package com.livyondocker

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.{size, udf}

import java.io.File
import java.nio.file.Paths
import scala.io.Source


object LivyOnDockerApp {

  case class Poem(title: String, contents: String)

  def main(args: Array[String]): Unit = {

    val spark = LivyOnDockerUtils.getSparkSession("LivyOnDockerApp")

    import spark.implicits._

    val dataSeq = getData(args(0))
    val dataset = spark.createDataset(dataSeq).as[Poem]
    val tokenizeUDF = udf(tokenize _)

    new StopWordsRemover()
      .setInputCol("tokenized")
      .setOutputCol("words")
      .transform(dataset.withColumn("tokenized", tokenizeUDF('contents)))
      .select('title, size('words).as("wordCount"))
      .sort('wordCount.desc)
      .show(false)
  }


  def tokenize(contents: String): Array[String] = {
    contents.trim
      .replaceAll("-", " ")
      .replaceAll("[\\p{Punct}]", "")
      .toLowerCase
      .split("\\s+")
      .distinct
  }

  def getData(path: String): List[Poem] = {

    new File(path).listFiles.toList.map(f => {

      val basename = Paths
        .get(f.toString)
        .getFileName.toString
        .split("\\.").head

      val source = Source.fromFile(f.toString)
      val contents = source.mkString

      source.close()

      Poem(basename, contents)

    })
  }
}
