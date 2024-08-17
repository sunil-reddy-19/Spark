from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd

def spark_init():
    spark = SparkSession.builder.appName("Spark_1").getOrCreate()
    return spark

def read_input_file(fileName,spark):
    df = spark.read.csv(fileName,header = True, inferSchema=True)
    return df

spark = spark_init()

file_name_Sales = "Input_Files/CaseStudy_1_Input/Global Superstore Sales - Global Superstore Sales.csv"
file_name_Return = "Input_Files/CaseStudy_1_Input/Global Superstore Sales - Global Superstore Returns.csv"
df_Sales = read_input_file(file_name_Sales,spark)
df_Return = read_input_file(file_name_Return,spark)

print("--------Sales sample data---------")
print("\n")
df_Sales.show()
print("\n")
print("--------Return sample data---------")
df_Return.show()
print("\n")

df_join = df_Sales.join(df_Return,df_Sales["Order ID"] == df_Return["Order ID"],"Left").drop(df_Return["Order ID"])

#df_join.show()

df_filter_NonReturn = df_join.filter(df_join["Returned"].isNull() )

#df_filter_NonReturn.show()

#print(df_filter_NonReturn.count())

df_filter_NonReturn = (df_filter_NonReturn.withColumn("Month",split(col("Order Date"),"/").getItem(0))
                       .withColumn("Year",split(col("Order Date"),"/").getItem(2))
                       .withColumn("Profit_New",regexp_replace(col("Profit"),"\$","")))

#df_filter_NonReturn.show()

df_grp_sel = df_filter_NonReturn.select("Year","Month","Category","Sub-Category",cast("Int","Quantity"),cast("float","Profit_New"))

#df_grp_sel.show()

df_Sum_Quamt_Profit = df_grp_sel.groupby("Year","Month","Category","Sub-Category").agg(sum("Quantity").alias("Total Quantity Sold"),sum("Profit_New").alias("Total Profit"))
df_Sum_Quamt_Profit = df_Sum_Quamt_Profit.orderBy("Year","Month","Category","Sub-Category")
# Output of final Dataframe
print("-------- Final DataFrame output---------")
print("\n")
df_Sum_Quamt_Profit.show()
print("\n")

## Unit Test Case function

def unit_Test(df_source,df_output,year,month,category,sub_category):

    df_source_unit = df_source.select("Year","Month","Category","Sub-Category",cast("Int","Quantity"),cast("float","Profit_New"))
    df_source_unit_spec = df_source_unit.filter((df_source_unit["Year"] == year) & (df_source_unit["Month"] == month) &
                                                (df_source_unit["Category"] == category) & (df_source_unit["Sub-Category"] == sub_category))
    df_source_unit_spec_grp = (df_source_unit_spec.groupby("Year","Month","Category","Sub-Category")
                               .agg(sum("Quantity").alias("Total Quantity Sold"),sum("Profit_New").alias("Total Profit")))

    df_output_unit_spec = df_output.filter(
        (df_output.Year == year) & (df_output.Month == month) & (df_output.Category == category) & (
                    df_output['Sub-Category'] == sub_category))
    #df_output_unit_spec.show()

    pd_df_output = df_output_unit_spec.toPandas()

    pd_df_source = df_source_unit_spec_grp.toPandas()

    if (pd_df_output['Total Profit'][0] == pd_df_source['Total Profit'][0]) & (pd_df_output['Total Quantity Sold'][0] == pd_df_source['Total Quantity Sold'][0]):
        return "Unit TestCase Passed for category " + category + " and sub_category " + sub_category + " for Date " + year + "/" + month
    else:
        return "Unit TestCase Failed for category " + category + " and sub_category " + sub_category + " for Date " + year + "/" + month


# Unit Test Case
df_unitTest = unit_Test(df_filter_NonReturn,df_Sum_Quamt_Profit,'2012','1','Technology','Phones')



print("-------------Unit TestCase Result-----------------")
print("\n")
print(df_unitTest)
print("\n")
print("-------------Unit TestCase Result-----------------")
