import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object Scala_Spark_1 {

  def spark_init(): SparkSession = {
     val spark = SparkSession.builder()
       .appName("Scala_Spark")
       .master("local[1]")
       .getOrCreate()
    spark
  }

  def read_file(fileName:String,spark:SparkSession) = {
    val df = spark.read.option("header","true").option("inferSchema","true").csv(fileName)
    df
  }

  def main(args:Array[String]): Unit = {

    val spark = spark_init()

    val filename="/Users/sunil_reddy/Spark/input_data/datasets/dw_dataset/sales_1.csv"

    var df1 = read_file(filename,spark)

    df1.show()

    val df_grp = df1.groupBy("date_of_sale")
      .agg(sum("total_amount")
        .alias("total_sum_by_date"))

    df_grp.show()

    var df_2 = df1.withColumn("dates",date_format(col("date_of_sale"),"dd/mm/yyyy"))

    //df_2.select(date_format(col("date_of_sale"),"MM-dd-yyyy").as("date_1")).show()

    df_2.show()

    df_2.printSchema()



  }

}
