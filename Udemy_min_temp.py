from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("Min_Temp")

sc = SparkContext(conf=conf)

def processLines(line):
    fields = line.split(',')
    stationId = fields[0]
    date = fields[1]
    type = fields[2]
    temp = float(int(fields[3]) * 0.1 * (9.0/5.0) + 32)
    return (stationId,date,type,temp)


lines = sc.textFile("Input_Files/1800.csv")

parsedLines = lines.map(processLines)
tempminlines = parsedLines.filter(lambda x: 'TMIN' in x[2])
templines = tempminlines.map(lambda x : (x[0],x[3]))
mintemp = templines.reduceByKey(lambda x,y : min(x,y))

print(parsedLines.take(10))
print(tempminlines.take(10))
print(templines.take(10))
print(mintemp.collect())