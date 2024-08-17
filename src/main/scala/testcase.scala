import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sum}

object testcase {

  def unit_TestCase(df_source: DataFrame, df_output: DataFrame, year: String, month: String, category: String, sub_category: String): DataFrame = {

    val df_spe_MMYY = df_source.filter(col("Year") === year && col("Month") === month)

    val df_spe_MMYY_cate = df_spe_MMYY.filter(col("Category") === category && col("Sub-Category") === sub_category)

    //df_spe_MMYY_cate.show()

    val count = df_spe_MMYY_cate.count()

    println(f"count of specific category: " + count)

    val df_source_unit_output = df_spe_MMYY_cate.groupBy(col("Year"), col("Month"), col("Category"), col("Sub-Category")).agg(sum(col("Quantity")).as("Total Quantity Sold"), sum(col("Profit_New")).as("Total Profit"))

    df_source_unit_output.show()


    val df_output_actual_unit = df_output.filter(col("Year") === year && col("Month") === month && col("Category") === category && col("Sub-Category") === sub_category)

    df_output_actual_unit.show()

    val df_unitTest_comp = df_output_actual_unit.join(df_source_unit_output,
      df_output_actual_unit("Year") === df_source_unit_output("Year") && df_output_actual_unit("Month") === df_source_unit_output("Month") &&
        df_output_actual_unit("Category") === df_source_unit_output("Category") &&
        df_output_actual_unit("Sub-Category") === df_source_unit_output("Sub-Category"), "left").select(df_output_actual_unit("Year"), df_output_actual_unit("Month"),
      df_output_actual_unit("Category"), df_output_actual_unit("Sub-Category"), df_output_actual_unit("Total Quantity Sold"),
      df_output_actual_unit("Total Profit"), df_source_unit_output("Total Quantity Sold").as("RightTB_Total_Quantity_Sold"),
      df_source_unit_output("Total Profit").as("RightTB_Total_Profit"))

    df_unitTest_comp.show()

    var a = df_unitTest_comp.map{ x =>

      if (x(5) == x(7)) {
        "Unit TestCase Passed for category " + category + " and sub_category " + sub_category + " for Date " + year + "/" + month
      }
      else {
        "Unit TestCase Failed for category " + category + " and sub_category " + sub_category + " for Date " + year + "/" + month
      }

    }



  }

}
