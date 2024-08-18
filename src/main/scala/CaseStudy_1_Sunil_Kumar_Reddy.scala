import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions._
import org.apache.logging.log4j._



object CaseStudy_1_Sunil_Kumar_Reddy {

  def spark_init(): SparkSession = {

    val spark = SparkSession.builder().appName("CaseStudy").master("local[1]").getOrCreate()
    spark

  }

  def fileRead(fileName:String,spark:SparkSession): DataFrame = {

    val df = spark.read.option("header","true").option("inferSchema","true").csv(fileName)
    df

  }

  def unitTest(spark:SparkSession,df_Source:DataFrame,df_return:DataFrame,df_Target:DataFrame,year:String,month:String,category:String,sub_category:String): Unit = {

    import spark.implicits._

    var df_Source_unitTest = df_Source.select($"Order ID",$"Returns",lpad(split(col("Order Date"),"/").getItem(0),2,"0").as("Month"),
      lpad(split(col("Order Date"),"/").getItem(2),4,"20").as("Year"),$"Category",$"Sub-Category",$"Quantity",$"Profit")

    var df_Source_Sel_UnitTest = df_Source_unitTest.filter(col("Year") === year && col("Month") === month && col("Category") === category &&
    col("Sub-Category") === sub_category)

    var df_Source_UT_NR = df_Source_Sel_UnitTest.join(df_return,df_Source_Sel_UnitTest("Order ID") === df_return("Order ID"),"left").filter(col("Returned").isNull)

    df_Source_UT_NR = df_Source_UT_NR.withColumn("Profit_New",regexp_replace(col("Profit"),"[$]","").cast("Double"))

    var df_Source_UT_NR_Output = df_Source_UT_NR.groupBy($"Year",$"Month",$"Category",$"Sub-Category").agg(sum($"Quantity").as("Total Quantity Sold"),sum($"Profit_New").as("Total Profit"))

    //df_Source_UT_NR_Output.show()

    var df_Target_UT = df_Target.filter(col("Year") === year && col("Month") === month &&
    col("Category") === category && col("Sub-Category") === sub_category)

    //df_Target_UT.show()

    val df_unitTest_comp = df_Target_UT.join(df_Source_UT_NR_Output,
      df_Target_UT("Year") === df_Source_UT_NR_Output("Year") && df_Target_UT("Month") === df_Source_UT_NR_Output("Month") &&
        df_Target_UT("Category") === df_Source_UT_NR_Output("Category") &&
        df_Target_UT("Sub-Category") === df_Source_UT_NR_Output("Sub-Category"),"left").select(df_Target_UT("Year"),df_Target_UT("Month"),
      df_Target_UT("Category"),df_Target_UT("Sub-Category"),df_Target_UT("Total Quantity Sold"),
      df_Target_UT("Total Profit"),df_Source_UT_NR_Output("Total Quantity Sold").as("RightTB_Total_Quantity_Sold"),
      df_Source_UT_NR_Output("Total Profit").as("RightTB_Total_Profit"))

    // Test case data
    println("Printing the unit test case DataFrame.....")
    df_unitTest_comp.show()
    println("\n")


    var testResult = df_unitTest_comp.map{ x =>

      if ((x(5) == x(7)) && (x(4) == x(6))) {
        "Unit Test case Passed for category: " + category + " and sub category " + sub_category + " for date " + year + "/" + month
      }

      else {
        "Unit Test case Failed for category: " + category + " and sub category " + sub_category + " for date " + year + "/" + month
      }

    }

    println("---------- Unit Case Result -------------- ")
    println("\n")

    testResult.foreach(println)

    println("\n")
    println("---------- Unit Case Result -------------- ")



  }




  def main(args:Array[String]): Unit = {

    //initializing the spark session through spark_init() function
    val spark = spark_init()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._



    val filename_sales = "input_data/datasets/CaseStudyData_1/Global Superstore Sales - Global Superstore Sales.csv"
    val filename_return = "input_data/datasets/CaseStudyData_1/Global Superstore Sales - Global Superstore Returns.csv"

    //reading the input files using the fileName function
    var df_sales = fileRead(filename_sales,spark)

    var df_return = fileRead(filename_return,spark)

    // Sample data of sales DataFrame
    println("Sales dataframe sample data.....")
    println("\n")
    df_sales.show()
    // Sales DataFrame schema
    println("sales df schema....")
    df_sales.printSchema()
    // return dataframe schema
    println("return df schema....")
    df_return.printSchema()

    //selecting the required columns for calculation and order date column is splitted into month and year
    var df_sales_sel = df_sales.select($"Order ID",$"Returns",lpad(split(col("Order Date"),"/").getItem(0),2,"0").as("Month"),
      lpad(split(col("Order Date"),"/").getItem(2),4,"20").as("Year"),$"Category",$"Sub-Category",$"Quantity",$"Profit")


    //removing the $ sign from profit column
    df_sales_sel = df_sales_sel.withColumn("Profit_New",regexp_replace(col("Profit"),"[$]","").cast("Double"))


    //Joining the sales with return DF and filtering the data which is returned
    var df_sales_sel_NotReturn = df_sales_sel.join(df_return,df_sales_sel("Order ID") === df_return("Order ID"),"left").filter(col("Returned").isNull)


    //df_sales_sel_NotReturn.show()

    var df_output = df_sales_sel_NotReturn.groupBy($"Year",$"Month",$"Category",$"Sub-Category").agg(sum($"Quantity").as("Total Quantity Sold"),sum($"Profit_New").as("Total Profit"))

    df_output = df_output.orderBy("Year","Month","Category","Sub-Category")

    println("Printing the Output........")
    println("\n")
    df_output.show()

    //Writing into File with partitions

    //df_output.write.option("header","true").partitionBy("Year","Month").mode("overWrite").csv("output_data/CaseStudy_1")


    //  Testing the Data by unitTest funcation

    unitTest(spark,df_sales,df_return,df_output,"2012","01","Technology","Phones")


  }

}
