import org.apache.spark.sql.{Dataset,SparkSession}
object Spark_1 {

  def main(args:Array[String]): Unit = {

    val filename = "/Users/sunil_reddy/Spark/input_data/insurance.csv"

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .config("spark.driver.bindaddress", "127,0,0,1")
      .getOrCreate()

    val data:Dataset[String] = spark.read.option("header","true").textFile(filename)
    data.show()

  }

}
