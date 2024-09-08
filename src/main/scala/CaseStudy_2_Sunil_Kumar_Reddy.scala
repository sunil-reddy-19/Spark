import org.apache.spark.sql.functions.{avg, expr, window}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object CaseStudy_2_Sunil_Kumar_Reddy {

  def main(args:Array[String]): Unit = {

    def init_sparkSession() = {
      val spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("SparkStreaming")
        .getOrCreate()
      spark
    }

    def readIoTData(spark:SparkSession,fileName:String) = {

      val IoTInputSchema:StructType = new StructType()
        .add(StructField("device_id",DataTypes.StringType,false))
        .add(StructField("temperature",DataTypes.IntegerType,false))
        .add(StructField("timestamp",DataTypes.TimestampType,false))


      val inputIoTDF = spark
        .readStream
        .schema(IoTInputSchema)
        .option("header","true")
        .csv(fileName)

      inputIoTDF
    }

    def readStaticFile(spark:SparkSession,fileName:String)= {
      val staticDf:DataFrame = spark
        .read
        .option("header","true")
        .option("inferSchema","true")
        .csv(fileName)

      staticDf
    }

    val spark = init_sparkSession()

    spark.sparkContext.setLogLevel("warn")

    import spark.implicits._

    val IotDF = readIoTData(spark,"/Users/sunil_reddy/Scala/Case_Study_2/IoTStreaming_Input/")
    val StaticDF = readStaticFile(spark,"/Users/sunil_reddy/Scala/Case_Study_2/IoTStatic_File/IoTStaticFile.csv")

    StaticDF.show()
    val iotDFWindow10 = IotDF.withWatermark("timestamp","10 minutes")

    val ioTWindowGrp = iotDFWindow10.groupBy(window($"timestamp","10 minutes"),$"device_id").agg(avg($"temperature").as("Avg_Temp"))

    val ioTJoin = ioTWindowGrp.join(StaticDF,ioTWindowGrp.col("device_id") === StaticDF.col("device_id"),"leftOuter")
      .drop(StaticDF.col("device_id"))

    val ioTMaxTempDevices = ioTJoin.filter($"Avg_Temp" > $"max_temp")



    val output = ioTMaxTempDevices.writeStream.outputMode("update").format("console").start()

    output.awaitTermination()



  }

}
