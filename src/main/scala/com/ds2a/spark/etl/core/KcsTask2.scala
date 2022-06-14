package com.ds2a.spark.etl.core

import org.apache.spark.sql.SparkSession

case class Learner(name:String,email:String,city:String);

object KcsTask2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("KcsTask2").getOrCreate()
    val courseData = Seq((1001,"Hadoop",7000 ),(1002,"Spark",7000),(1003,"Cassandra",7000),(1004,"Python",7000))
    import spark.implicits._
    val CourseDf = courseData.toDF("course_id","course_name","course_fee")

    val LearnersData = Array(
      Learner("Amit","amit@hadoopexam.com","Mumbai"),
      Learner("Rakesh","rakesh@hadoopexam.com","Pune"),
      Learner( "Jonathan","jonathan@hadoopexam.com","NewYork"),
      Learner( "Michael","michael@hadoopexam.com","Washington"),
      Learner("Simon","simon@hadoopexam.com","HongKong" ),
      Learner("Venkat","venkat@hadoopexam.com","Chennai"),
      Learner( "Roshni","roshni@hadoopexam.com","Banglore")
    )

    val learnersDataRdd = spark.sparkContext.parallelize(LearnersData)
    //RDD to DataFrame
    val LearnersDf = spark.createDataFrame(learnersDataRdd)
    LearnersDf.show(false)
    // DataFrame to DataSet
    val LearnerDs = LearnersDf.as[Learner]
    LearnerDs.show(false)
  }
}
