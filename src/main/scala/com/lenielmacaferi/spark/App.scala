package com.lenielmacaferi.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import com.databricks.spark.xml

object ProcessXml {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
      .setAppName("ProcessXml")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)

    loadBookData(sqlContext)
  }

  def loadBookData(sqlContext: SQLContext) = {
    var df: DataFrame = null
    var newDf: DataFrame = null

    import sqlContext.implicits._

    df = sqlContext.read
      .format("xml")
      .option("rowTag", "book")
      .load("data/books.xml")

    df.printSchema()
  }
}