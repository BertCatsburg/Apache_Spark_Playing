from pyspark import SparkConf, SparkContext


def parseLine(line):
    fields = line.split(',')
    stationId = fields[0]
    # dateOfMeasure = fields[1]
    entryType = fields[2]
    temperature = float(fields[3]) / 10
    return stationId, entryType, temperature


# ** Config Spark
conf = SparkConf().setMaster("local").setAppName("WeatherFiltering")
sc = SparkContext(conf=conf)

# ** Logging
log4jLogger = sc._jvm.org.apache.log4j
logging = log4jLogger.LogManager.getLogger('*** ')
logging.info('About to start')

# ** Read data
lines = sc.textFile("file:///Udemy_2_and_3/weather-data-1800.csv")  # lines is a RDD
parsedLines = lines.map(parseLine)  # parsedLines = RDD
logging.info(f"number of lines = {len(parsedLines.collect())}")

# for i in parsedLines.collect()[:5]:
#     logging.info(f"Line from datafile: {i}")

# ** Filter only MIN-TEMPS
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
logging.info(f"number of mintemps = {len(minTemps.collect())}")

# ** Remove Min-Temps since we already filtered those
stationTempsMin = minTemps.map(lambda x: (x[0], x[2]))
# for i in stationTempsMin.collect()[:10]:
#     logging.info(f"StationTemp, only MinTemp: {i}")

# ** Take the minimum temp for each station across the whole period
minTempsAggregated = stationTempsMin.reduceByKey(lambda x, y: min(x, y))
results = minTempsAggregated.collect()
for result in results:
    logging.info(f"{result[0]}\t{result[1]} Celcius")

