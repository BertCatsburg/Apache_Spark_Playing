from pyspark import SparkConf, SparkContext

# ** Config Spark
conf = SparkConf().setMaster("local").setAppName("WordCounter")
sc = SparkContext(conf=conf)

# ** Logging
log4jLogger = sc._jvm.org.apache.log4j
logging = log4jLogger.LogManager.getLogger('*** ')
logging.info('About to start')

# ** Read data and split on Words
input = sc.textFile("file:///Udemy_2_and_3/Book.txt")  # lines is a RDD
# for x in input.collect()[:20]:
#     print(f"Line is {x}")
wordRDD = input.flatMap(lambda x: x.split())  # parsedLines = RDD
logging.info(f"number of Words = {len(wordRDD.collect())}")
for x in wordRDD.collect()[:20]:
    print(f"A word is {x}")

wordCounts = wordRDD.countByValue()
wordCountsSorted = sorted(wordCounts.items(), key=lambda k: k[1], reverse=True)
for word, count in wordCountsSorted:
    cleanWord = word.encode('ascii', 'ignore').decode('ascii', 'ignore')
    if count > 10:
        print(f"{str(cleanWord)} - {count}")
