from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

spark = SparkSession.builder.appName("super_hero").getOrCreate()

schema = StructType([StructField("id",IntegerType(),nullable=False),
                     StructField("name",StringType(),nullable=True)])

lines = spark.read.text("Input_Files/Marvel_Graph")

#lines.show(10,truncate=False)

names = spark.read.option("sep"," ").schema(schema).csv("Input_Files/Marvel_Names.txt")

#names.show(10,truncate=False)

connection = lines.withColumn("id", func.split(func.col("value"), ' ')[0]).withColumn("connections", func.size(func.split(func.col("value"), ' ')) - 1)
connection.show(n=10, truncate=False)
dfconnection = connection.groupby("id").agg(func.sum("connections").alias("connections")).sort(func.col("connections").desc())
print(type(dfconnection))
dfHighestConn = dfconnection.first()

dfHighestConnNames = names.filter(func.col("id") == dfHighestConn.id)

dfHighestConnNames.show(truncate=False)

dflowestConn = dfconnection.filter(func.col("connections") == 1)

dflowestConn.show(truncate=False)

dflowestConnNames = names.join(dflowestConn,names.id == dflowestConn.id).drop(dflowestConn.id)

dflowestConnNames.show(truncate=False)
