package com.ds2a.spark.etl.sql

import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

object createAnilDf {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val data = Seq(Row("James","","Smith","1991-04-01","M",3000),
    Row("Michael","Rose","","20000519","M",4000),
    Row("Robert","","Williams","19780905","M",4000),
    Row("Maria","Anne","Jones","19671201","F",4000),
    Row("Jen","Mary","Brown","19800217","F",-1))
    val data1 = Seq(("James","","Smith","1991-04-01","M",3000),
      ("Michael","Rose","","20000519","M",4000),
      ("Robert","","Williams","19780905","M",4000),
      ("Maria","Anne","Jones","19671201","F",4000),
      ("Jen","Mary","Brown","19800217","F",-1))
    val schema = StructType(Array(StructField("firstname",StringType,nullable = true),StructField("middlename",StringType,nullable = true),StructField("lastname",StringType,nullable = true),StructField("dob",StringType,nullable = true),StructField("gender",StringType,nullable = true),StructField("salary",IntegerType,nullable = true)))
    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
    import scala.collection.JavaConversions._
    import spark.implicits._
    val df = spark.createDataFrame(data,schema)
    val df1 = spark.createDataFrame(data1).toDF(columns:_*)
    df.show()
    df.printSchema()
    df1.printSchema()
    df1.show()
  }
}
