import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}

object Streaming_1 {

  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Streaming_1")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")

    val joinInputDf = spark.read.option("header","true").option("inferSchema","true").csv("/Users/sunil_reddy/Scala/dataset/Streaming_InputJoin.txt")
    joinInputDf.show()

    val streamingDf = spark.readStream.text("/Users/sunil_reddy/Scala/Streaming_Data/")

    val wordsDf = streamingDf.withColumn("words",functions.split(streamingDf.col("value")," "))
    val wordDf = wordsDf.withColumn("word",explode(wordsDf.col("words")))
    val groupByDf = wordDf.groupBy(wordDf.col("word")).count()
    val joinDF = groupByDf.join(joinInputDf,groupByDf.col("word") === joinInputDf.col("word"),joinType = "left")
    val streamOut = joinDF.writeStream.outputMode("complete").format("console").start()

    //streamOut.awaitTermination()

    println(streamOut.isActive)

    streamOut.awaitTermination()

  }

}
