package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructField, StructType, TimestampType}

object jsonData {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val df = spark.read.option("header",false).option("delimiter","|").csv("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\input\\jsondata.txt")
    val df1 = df.toDF("value")
    /*val df2 = df1.withColumn("value",from_json(col("value"),MapType(StringType,StringType)))
    df2.printSchema()
    df2.show(truncate = false)*/
    val schema = new StructType(Array(StructField("datetime",StringType, true)
      ,StructField("userid",StringType, true)
      ,StructField("country", StringType, true)
      ,StructField("sessionid", StringType, true)
      ,StructField("http_method", StringType, true)
      ,StructField("url", StringType, true)))

    val df3 = df1.withColumn("value",from_json(col("value"),schema))
    val df4 = df3.select(col("value.*"))
    df4.printSchema()
    df4.show(truncate = false)
import spark.implicits._
    val distinctDf = df4.distinct()
    distinctDf.show()

    val distinctDfWrtuserid = df4.select(df4("userid")).distinct
    println("Distinct count: "+distinctDfWrtuserid.count())
    distinctDfWrtuserid.show(false)
  }
}