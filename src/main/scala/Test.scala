import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .config("spark.driver.bindaddress", "127,0,0,1")
      .getOrCreate();
    println(spark)
    println("Spark Version : " + spark.version)

    val df = spark.read.option("header",value = "True").option("inferSchema",value = "True").csv("input_data/insurance.csv")
    df.show()





  }
}
