from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)
sc.setLogLevel("WARN")

log4jLogger = sc._jvm.org.apache.log4j
logging = log4jLogger.LogManager.getLogger('*** BERTLOG')
logging.warn('About to start')

lines = sc.textFile("file:///Udemy_2_and_3/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
logging.warn('Result for CountByValue')
logging.warn(result)
logging.warn(result.items())
logging.warn(sorted(result.items(), key=lambda x: x[1], reverse=True))

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    logging.warn(f"Key = {key} with value {value}")
