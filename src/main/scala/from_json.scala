
import org.apache.spark.sql.SparkSession

object from_json {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()

    val jsonString ="""{"datetime":"2015-12-13 00:03:50.347","userid":"492","country":"canada","sessionid":"dbfa3e2f6a274e3285fc99a7610edcad","http_method":"GET","url":"/logout"}"""


  }
}