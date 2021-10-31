from pyspark import SparkConf, SparkContext
import collections
import itertools

conf = SparkConf().setMaster("local").setAppName("UsersHistogram")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

log4jLogger = sc._jvm.org.apache.log4j
logging = log4jLogger.LogManager.getLogger('*** BERTLOG')
print('*** About to start UsersHistogram')

lines = sc.textFile("file:///Udemy_2_and_3/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[0])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items(), key=lambda x: x[1], reverse=True))
x = itertools.islice(sortedResults.items(), 20)

for key, value in x:
    print(f"*** User = {key} Voted {value} times")
