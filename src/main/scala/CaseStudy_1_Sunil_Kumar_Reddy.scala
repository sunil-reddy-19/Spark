import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions._



object CaseStudy_1_Sunil_Kumar_Reddy {

  def spark_init(): SparkSession = {

    val spark = SparkSession.builder().appName("CaseStudy").master("local[1]").getOrCreate()
    spark

  }

  def fileRead(fileName:String,spark:SparkSession): DataFrame = {

    val df = spark.read.option("header","true").option("inferSchema","true").csv(fileName)
    df

  }

  def main(args:Array[String]): Unit = {

    //initializing the spark session through spark_init() function
    val spark = spark_init()

    import spark.implicits._


    //reading the input files using the fileName function
    val filename_sales = "input_data/datasets/CaseStudyData_1/Global Superstore Sales - Global Superstore Sales.csv"
    val filename_return = "input_data/datasets/CaseStudyData_1/Global Superstore Sales - Global Superstore Returns.csv"

    var df_sales = fileRead(filename_sales,spark)

    var df_return = fileRead(filename_return,spark)

    df_sales.show()

    df_sales.printSchema()

    df_return.printSchema()

    //selecting the required columns for calculation
    var df_sales_sel = df_sales.select($"Order ID",$"Returns",lpad(split(col("Order Date"),"/").getItem(0),2,"0").as("Month"),
      lpad(split(col("Order Date"),"/").getItem(2),4,"20").as("Year"),$"Category",$"Sub-Category",$"Quantity",$"Profit")



    df_sales_sel = df_sales_sel.withColumn("Profit_New",regexp_replace(col("Profit"),"[$]","").cast("Int"))

    df_sales_sel.printSchema()


    var df_sales_sel_filter = df_sales_sel.where(col("Returns").contains("No"))

    //df_sales_sel_filter.show()

    var df_sales_sel_NotReturn = df_sales_sel.join(df_return,df_sales_sel("Order ID") === df_return("Order ID"),"left").filter(col("Returned").isNull)


    //df_sales_sel_NotReturn.show()

    var df_output = df_sales_sel_NotReturn.groupBy($"Year",$"Month",$"Category",$"Sub-Category").agg(sum($"Quantity").as("Total Quantity Sold"),sum($"Profit_New").as("Total Profit"))

    df_output.orderBy("Year","Month").show()






  }

}
