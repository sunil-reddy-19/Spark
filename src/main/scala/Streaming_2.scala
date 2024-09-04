import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Streaming_2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Streaming_1")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")


    def read_news(spark: SparkSession): DataFrame = {
      val newsSchema: StructType = new StructType()
        .add(new StructField("ticker", DataTypes.StringType, false))
        .add(new StructField("news", DataTypes.StringType, false))
        .add(new StructField("date_of_news", DataTypes.TimestampType, true))

      val news = spark.readStream.schema(newsSchema).json("file:///C:\\tmp\\news_json")
      news
    }

    def read_price(spark: SparkSession): DataFrame = {
      val priceSchema: StructType = new StructType()
        .add(new StructField("ticker", DataTypes.StringType, false))
        .add(new StructField("price", DataTypes.DoubleType, false))
        .add(new StructField("price_time", DataTypes.TimestampType, true))

      val price = spark.readStream.schema(priceSchema)
        .option("header", "true").csv("file:///C:\\tmp\\price")
      price
    }

  }
}