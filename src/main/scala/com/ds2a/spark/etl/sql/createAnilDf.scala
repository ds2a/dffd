package com.ds2a.spark.etl.sql

import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object createAnilDf {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val data = Seq(Row("James","","Smith","1991-04-01","M",3000),
    Row("Michael","Rose","","2000-05-19","M",4000),
    Row("Robert","","Williams","1978-09-05","M",4000),
    Row("Maria","Anne","Jones","1967-12-01","F",4000),
    Row("Jen","Mary","Brown","1980-02-17","F",-1))
    val schema = StructType(Array(StructField("firstname",StringType,nullable = true),StructField("middlename",StringType,nullable = true),StructField("lastname",StringType,nullable = true),StructField("dob",StringType,nullable = true),StructField("gender",StringType,nullable = true),StructField("salary",IntegerType,nullable = true)))
    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
    import scala.collection.JavaConversions._
    val df = spark.createDataFrame(data,schema)
    df.printSchema()
    df.show()

  }
}
