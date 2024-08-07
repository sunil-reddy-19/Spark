
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SparkSession,DataFrame, functions}
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

    val sales_1_path = "/Users/sunil_reddy/Spark/input_data/datasets/dw_dataset/sales_1.csv"
    val sales_2_path = "/Users/sunil_reddy/Spark/input_data/datasets/dw_dataset/sales_2.csv"
    val product_path = "/Users/sunil_reddy/Spark/input_data/datasets/dw_dataset/product_meta.csv"



    val sales1Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val unionDf = sales1Df.unionAll(sales2Df)
    val prodDf: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    val joinedDf = prodDf.join(unionDf, "item_id")

    joinedDf.show()

    val df2 = sales1Df.select("item_qty", "unit_price", "total_amount")
    val df3 = df2.withColumn("actual_total", df2.col("item_qty") * df2.col("unit_price"))
    val df4 = df3.withColumn("discount", df3.col("actual_total") - df3.col("total_amount"))

    joinedDf.createOrReplaceTempView("ITEM_SALES")
    val sqlStr = "select item_id, product_name, " +
      "sum(total_amount) over (partition by product_name) as product_total, total_amount, " +
      "sum(total_amount) over() as grand_total from ITEM_SALES "
    val out = spark.sql(sqlStr)

    out.createOrReplaceTempView("ITEM_SALES_1")
    val output = spark.sql("select item_id,product_name,product_type, " +
      "rank(product_name) over(partition by product_type, order by product_total desc) as rank" +
      "from ITEM_SALES_1")

    output.show()







  }

}
