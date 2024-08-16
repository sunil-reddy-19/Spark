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

df_Sales.show()
df_Return.show()

df_join = df_Sales.join(df_Return,df_Sales["Order ID"] == df_Return["Order ID"],"Left").drop(df_Return["Order ID"])

#df_join.show()

df_filter_NonReturn = df_join.filter(df_join["Returned"].isNull())

df_filter_NonReturn.show()

print(df_filter_NonReturn.count())

df_filter_NonReturn = (df_filter_NonReturn.withColumn("Month",split(col("Order Date"),"/").getItem(0))
                       .withColumn("Year",split(col("Order Date"),"/").getItem(2))
                       .withColumn("Profit_New",regexp_replace(col("Profit"),"\$","")))

df_filter_NonReturn.show()

df_grp_sel = df_filter_NonReturn.select("Year","Month","Category","Sub-Category",cast("Int","Quantity"),cast("float","Profit_New"))

df_grp_sel.show()

df_Sum_Quamt_Profit = df_grp_sel.groupby("Year","Month","Category","Sub-Category").agg(sum("Quantity").alias("Total Quantity Sold"),sum("Profit_New").alias("Total Profit"))
df_Sum_Quamt_Profit = df_Sum_Quamt_Profit.orderBy("Year","Month","Category","Sub-Category")
df_Sum_Quamt_Profit.show()

