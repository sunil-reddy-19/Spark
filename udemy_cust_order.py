from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

spark = SparkSession.builder.appName("Cust_order").getOrCreate()

schema = StructType([StructField("cust_id",IntegerType(),False),
                     StructField("item_id",IntegerType(),True),
                    StructField("amount_spent",FloatType(),False)])

dfInput = spark.read.schema(schema).csv("Input_Files/customer-orders.csv")

dfInput.printSchema()
print("\n")
print("Top 10 Records")
dfInput.show(10,False)
print("\n")
print("Amount spent by custmoers")
dfInput.groupby("cust_id").sum("amount_spent").orderBy("cust_id").show(10,False)




