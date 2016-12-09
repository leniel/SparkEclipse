package com.lenielmacaferi.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import com.databricks.spark.xml

object ProcessUsersXml {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
      .setAppName("ProcessUsersXml")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)

    loadUsersData(sqlContext)
  }

  def loadUsersData(sqlContext: SQLContext) = {
    var df: DataFrame = null
    var newDf: DataFrame = null

    import sqlContext.implicits._

    // spark-xml has a bug that doesn't allow us to use
    // self-closing tags as top-level rows (the format used by StackOverflow data dump XML)
    // https://github.com/databricks/spark-xml/issues/92
    // Solved this by transforming each row attribute in its own XML element using XSL stylesheet
    df = sqlContext.read
      .format("xml")
      .option("rowTag", "user")
      .load("data/Users100000.out.xml") // Reading the transformed XML file from the local disk

    // Displays the XML data structure
    df.printSchema()

    // Creating a TempView so that sqlContext can run queries against it
    df.createOrReplaceTempView("users")
    sqlContext.sql("""select * from users""") //.show()

    // Writing the query result to CSV for post processing.
    df
      .coalesce(1) // Writes to a single files instead of creating multiple file partitions
      .write.option("header", "true") // Add headers to the CSV
      .mode("overwrite") // Overwrites the file if it already exists
      .csv("data/users.csv") // Path and file name
  }
}