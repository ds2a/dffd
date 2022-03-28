package Demo_dir

import org.apache.spark.sql.SparkSession

object ReadAvroDriver {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder().master("local").appName("avro").getOrCreate()
    val df = spark.read.format("avro").load("C:\\Users\\gsuresh\\Downloads\\spark-scala-examples-master\\src\\main\\resources\\userdata1.avro")
    val df2 = df.drop("comments","cc","registration_dttm")
    import spark.implicits._
    val fillDF = df2.filter($"gender"==="Male")
    //df.write.format("csv").option("header",true).save("C:\\Users\\gsuresh\\Desktop\\avro\\")
    fillDF.show()

  }

}
