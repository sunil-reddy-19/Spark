from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("Test_Spark_conf")
sc = SparkContext(conf=conf)

print(sc)

inp = sc.textFile("Input_Files/customers.txt")

print(inp.top(10))

print(inp.map(lambda x:x.split(",")).take(10))