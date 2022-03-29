package com.ds2a.spark.etl.core

object WordCountprogram {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").appName("WordCountExample").getOrCreate()

    val inputRdd = spark.sparkContext.textFile("C:\\Users\\hp\\Desktop\\text1.txt")


    val inputSplitRdd = inputRdd.map(line => line.split(" ")).map(word => (word, 1))

    val inputSplitRdd1 = inputSplitRdd.reduceByKey((a, b) => a + b)

    inputSplitRdd1.foreach(println)
    //inputSplitRdd.foreach(println)

  }

}
