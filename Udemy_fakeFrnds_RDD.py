from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("FakeFrnds")

sc = SparkContext(conf=conf)

def lineParser(lines):
    field = lines.split(',')
    id = field[0]
    name = field[1]
    age = int(field[2])
    numFrnds = int(field[3])
    return (id,name,age,numFrnds)

lines = sc.textFile("Input_Files/fakefriends.csv")
parsedLines = lines.map(lineParser)
ageNnumFrnds = parsedLines.map(lambda x: (x[2],x[3]))

grpAgeNnumFrnds = ageNnumFrnds.reduceByKey(lambda x,y: x + y)
idgrpAgeNnumFrnds = grpAgeNnumFrnds.map(lambda x: (1,x))
maxNumFrnds = grpAgeNnumFrnds.sortBy(lambda x: x[1],ascending=False).take(1)
minNumFrnds = grpAgeNnumFrnds.sortBy(lambda x: x[1],ascending=True).take(1)

print(parsedLines.take(10))
print(ageNnumFrnds.take(10))
print(grpAgeNnumFrnds.take(10))
print(maxNumFrnds)
print(minNumFrnds)


#print(maxNumFrnds.collect())
#print(minNumFrnds.collect())