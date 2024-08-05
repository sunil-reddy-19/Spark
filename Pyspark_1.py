import pyspark.sql
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

file_name = "/Users/sunil_reddy/PycharmProjects/Pyspark/Input_Files/sales_1.csv"
df = read_input_file(file_name,spark)
df.show()

df.select("item_id","item_qty",date_format("date_of_sale","dd-MM-yyyy").alias("New_date_format_sales")).show()

data = [[1,"abc",{"hair":"black","eye":"brown"},[1000,10000]],
        [2,"def",{"hair":"white","eye":"black"},[2000,20000]],
        [3,"xyz",{"hair":"red","eye":"red"},[3000,30000]]]
print(data)

columns = ["ID","NAME","Attribute","salary"]

df_1 = spark.createDataFrame(data,columns)

df_1.show()

df_2 = df_1.select("ID","NAME",explode("Attribute"))
df_2.withColumnsRenamed({"key":"attribute","value":"color"}).show()
