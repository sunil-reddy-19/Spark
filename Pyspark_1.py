import pyspark.sql
from pyspark.sql import SparkSession
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