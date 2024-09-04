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

      val news = spark.readStream.schema(newsSchema).csv("/Users/sunil_reddy/Scala/Streaming_Data/News_files")
      news
    }

    def read_price(spark: SparkSession): DataFrame = {
      val priceSchema: StructType = new StructType()
        .add(new StructField("ticker", DataTypes.StringType, false))
        .add(new StructField("price", DataTypes.DoubleType, false))
        .add(new StructField("price_time", DataTypes.TimestampType, true))

      val price = spark.readStream.schema(priceSchema)
        .option("header", "true").csv("/Users/sunil_reddy/Scala/Streaming_Data/Price_files")
      price
    }

    def join_streaming(spark: SparkSession): Unit = {
      val priceDf = read_price(spark)
      val newsDf = read_news(spark)
      val joinedDf = priceDf.join(newsDf, priceDf.col("ticker") === newsDf.col("ticker"))
      val query = joinedDf.writeStream.outputMode("append").format("console").start
      query.awaitTermination()
    }

    join_streaming(spark)

  }
}