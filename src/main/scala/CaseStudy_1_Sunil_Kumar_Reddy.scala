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


    //removing the $ sign from profit column
    df_sales_sel = df_sales_sel.withColumn("Profit_New",regexp_replace(col("Profit"),"[$]","").cast("Double"))

    //df_sales_sel.printSchema()

    //filtering the data which is returned
    //var df_sales_sel_filter = df_sales_sel.where(col("Returns").contains("No"))


    //Joining the sales with return DF and filtering the data which is returned
    var df_sales_sel_NotReturn = df_sales_sel.join(df_return,df_sales_sel("Order ID") === df_return("Order ID"),"left").filter(col("Returned").isNull)


    //df_sales_sel_NotReturn.show()

    var df_output = df_sales_sel_NotReturn.groupBy($"Year",$"Month",$"Category",$"Sub-Category").agg(sum($"Quantity").as("Total Quantity Sold"),sum($"Profit_New").as("Total Profit"))

    df_output = df_output.orderBy("Year","Month","Category","Sub-Category")


    //  Unit Test

    val df_spe_MMYY = df_sales_sel_NotReturn.filter(col("Year") === "2012" && col("Month") === "01")

    val df_spe_MMYY_cate = df_spe_MMYY.filter(col("Category") === "Technology" && col("Sub-Category") === "Phones")

    df_spe_MMYY_cate.show()

    val count = df_spe_MMYY_cate.count()

    println(f"count of specific category{Technology/Phones of 01/2012}: " + count)

    val df_unit_output = df_spe_MMYY_cate.groupBy($"Year",$"Month",$"Category",$"Sub-Category").agg(sum($"Quantity").as("Total Quantity Sold"),sum($"Profit_New").as("Total Profit"))

    df_unit_output.show()

   //df_unit_output("Total Profit").equalTo("123")

    val df_output_actual_unit = df_output.filter(col("Year") === "2012" && col("Month") === "01" && col("Category") === "Technology" && col("Sub-Category") === "Phones")

    df_output_actual_unit.show()





  }

}
