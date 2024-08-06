import org.apache.spark.sql.catalyst.expressions.Lag
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object window_functions {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("windowFunc").master("local[1]").getOrCreate()
    println(spark)

    var df = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("/Users/sunil_reddy/Spark/input_data/datasets/dw_dataset/sales_1.csv")

    df.show()
    println(df.count)

    var total_sum_window = Window.partitionBy("item_id").orderBy("rownum")
    df = df.withColumn("rownum",row_number() over(Window.orderBy("item_id")))
    df = df.withColumn("total_qty",sum("item_qty") over(total_sum_window))
    df = df.withColumn("lag_amount",lag("total_amount",1,0) over(Window.orderBy("rownum")) )
    df = df.withColumn("lead_amount",lead("total_amount",1,0) over(Window.orderBy("rownum")) )
    df.show()

    df.createOrReplaceTempView("table_df")
    var df_1 = spark.sql("select item_id,item_qty,unit_price,total_amount,date_of_sale, " +
      "row_number() over (order by item_id) as rownum" +
      " from table_df")
    //df_1.show()
    df_1.createOrReplaceTempView("table_df_1")
    spark.sql("select item_id,item_qty,unit_price,total_amount,date_of_sale,rownum," +
      "sum(item_qty) over (partition by item_id) as total_qty," +
      "lag(total_amount,1,0) over(order by rownum) as lag_amount," +
      "lead(total_amount,1,0) over(order by rownum) as lead_amount" +
      " from table_df_1").show()




  }

}
