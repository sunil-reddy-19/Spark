from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

df = spark.read.csv("insurance.csv",inferSchema=True,header=True)

df.show()