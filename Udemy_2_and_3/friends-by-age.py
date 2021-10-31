from pyspark import SparkConf, SparkContext
import collections


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return age, numFriends


# lambda x, y: (x[0] + y[0], x[1] + y[1])
def reduceByKeyFunction(x, y):
    return x[0] + y[0], x[1] + y[1]


conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

print('*** About to start Friends-By-Age Program')

lines = sc.textFile('file:///Udemy_2_and_3/friends-by-age.csv')
rdd = lines.map(parseLine)

# Count up Sum of Friends and Number Of Entries per Age
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(reduceByKeyFunction)

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
sortedResults = collections.OrderedDict(sorted(results))

for x in sortedResults.items():
    print(f"Age {x[0]} with (on average) {int(x[1])} friends")



