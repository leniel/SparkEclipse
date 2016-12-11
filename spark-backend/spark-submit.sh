#!/bin/sh

echo "Submitting spark-backend JAR to Spark..." 

spark-submit --packages com.databricks:spark-xml_2.11:0.4.1 --class com.lenielmacaferi.spark.ProcessUsersXml --master local com.lenielmacaferi.spark-backend-0.0.1-SNAPSHOT.jar