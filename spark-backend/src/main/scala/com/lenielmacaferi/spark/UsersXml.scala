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

    import sqlContext.implicits._

    // spark-xml has a bug that doesn't allow us to use
    // self-closing tags as top-level rows (the format used by StackOverflow data dump XML)
    // https://github.com/databricks/spark-xml/issues/92
    // Solved this by transforming each row attribute in its own XML element using XSL stylesheet
    df = sqlContext.read
      .format("xml")
      .option("rowTag", "user")
      .load("data/Users10.out.xml") // For debugging purposes...
    //.load("data/Users100000.out.xml")// Reorganizing projects structure...

    // Displays the XML data structure
    df.printSchema()
   
    df = convertToDateDf(df)
    
    // Creating a TempView so that sqlContext can run queries against it
    df.createOrReplaceTempView("users")
    
    //getAllUsers(sqlContext)
    
    //getAllUsersOrderedByReputation(sqlContext)
    
    //getUsersCountByLocation(sqlContext)
    
    //searchAboutMe(sqlContext, "person")
    
    getUsersGroupedByRegistrationYear(sqlContext)
    
    getUsersGroupedByRegistrationMonthYear(sqlContext)
  }
  
  def getAllUsers(sqlContext: SQLContext) = {
    
    var df: DataFrame = sqlContext.sql("""SELECT * FROM users""")
        
    df.show()

    saveDfToJson(df, "AllUsers.json")
  }
  
  def getUsersGroupedByRegistrationYear(sqlContext: SQLContext) = {
    
    var df: DataFrame = sqlContext.sql("""SELECT YEAR(CD) as Year, COUNT(*) as Count FROM users GROUP BY YEAR(CD) ORDER BY Year""")
        
    df.show()

    saveDfToJson(df, "UsersGroupedByRegistrationYear.json")
  }
  
    def getUsersGroupedByRegistrationMonthYear(sqlContext: SQLContext) = {
    
    // Functions available:
    // https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html
    var df: DataFrame = sqlContext.sql("""SELECT DATE_FORMAT(CD, 'yyyy-MM') as MonthYear,
           COUNT(*) as Count FROM users GROUP BY DATE_FORMAT(CD, 'yyyy-MM') ORDER BY MonthYear""")
        
    df.show()

    saveDfToJson(df, "UsersGroupedByRegistrationMonthYear.json")
  }

  def getAllUsersOrderedByReputation(sqlContext: SQLContext) = {

    var df: DataFrame = sqlContext.sql("""SELECT DisplayName, Reputation FROM users ORDER BY Reputation""") //.show()

    df.show()

    saveDfToJson(df, "AllUsersOrderedByReputation.json")
  }
  
    def getUsersCountByLocation(sqlContext: SQLContext) = {
    
    var df: DataFrame = sqlContext.sql("""SELECT Location, COUNT(*) AS Count FROM users GROUP BY Location""")
        
    df.show()

    saveDfToJson(df, "UsersCountByLocation.json")
  }
    
 def searchAboutMe(sqlContext: SQLContext, search : String) = {
    
    var df: DataFrame = sqlContext.sql("""SELECT DisplayName, AboutMe FROM users WHERE LOWER(AboutMe) LIKE '%person%'""")
        
    df.show()

    saveDfToJson(df, "SearchAboutMe.json")
  }

  def saveDfToJson(df: DataFrame, pathName: String) = {

    val fullPath: String = "data/"+pathName
    
    // Writing the query result to CSV for post processing in spark-frontend.
    df.coalesce(1) // Writes to a single file instead of creating multiple file partitions
      .write.option("header", "true") // Add headers to the CSV
      .mode("overwrite") // Overwrites the file if it already exists
      .json("target/data/"+pathName) // Path where the CSV will be stored
  }
   
  def convertToDateDf(df: DataFrame) : DataFrame = {
    
    import org.apache.spark.sql.functions._
    
    val toDate = udf (
        (entry : String) => {
          
          var retVal : java.sql.Date = null
          
          val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
          
          try
          {
            retVal = new java.sql.Date(sdf.parse(entry).getTime)
          }
           catch
           {
             case e: Exception => {
               
               retVal = null
             }
            }
           
           retVal
        })
        
        df.withColumn("CD", toDate(df.col("CreationDate")))
        //select("DisplayName", "CreationDate")
  }

}