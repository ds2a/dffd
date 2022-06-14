package com.ds2a.spark.etl.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object KcsTask1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("KcsTask1").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val hecoursesDf = spark.read.option("header","true").option("multiline","true").json("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\input\\hecourses.json")
    hecoursesDf.printSchema()
    hecoursesDf.show(false)
     val schema = StructType(Array(
       StructField("StdID",StringType,nullable = true),
       StructField("Courseid",IntegerType,nullable = true),
       StructField("RegistrationDate",StringType,nullable = true)
     ))

    val studentsDf = spark.read.option("header","false").schema(schema).csv("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\input\\students.csv")
    studentsDf.printSchema()
    studentsDf.show(false)

    val CourseNotSubscribedDf = hecoursesDf.join(studentsDf,hecoursesDf("CourseId") === studentsDf("Courseid"),"leftAnti")
    CourseNotSubscribedDf.write.mode("overwrite").json("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\KcsTask1Target\\notsubscribed.json")
    CourseNotSubscribedDf.show(false)

    val CoursesSubscribedDf =  hecoursesDf.join(studentsDf,hecoursesDf("CourseId") === studentsDf("Courseid"),"inner")
    CoursesSubscribedDf.show(false)

    CoursesSubscribedDf.createTempView("CoursesSubscribedTable")
    val TotalFeeCollectedWrtCategoryDf = spark.sql("""select Category,sum(CourseFee) as TotalFeeCollected from CoursesSubscribedTable group by Category """).coalesce(1)
    TotalFeeCollectedWrtCategoryDf.show(false)
    TotalFeeCollectedWrtCategoryDf.write.mode("overwrite").json("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\KcsTask1Target\\TotalFee.json")
  }

}
