from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs


def loadMovieNames():
    movieNames = {}

    # Codecs.open : Open an Local Encoded file and return StreamReaderWriter
    with codecs.open("/sparkdata/ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            # Field 0 is the ID, Field 1 is the MovieTitle
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster
# sparkContext is of the old RDD Interface. (While SparkSession is the SQL interface aka DataFrames)
# We 'broadcast' whatever the loadMovieNames returns
# Referring to the BroadCast object as nameDict
# nameDict is the Broadcasted object, not the dictionary itself !!
# Type of nameDict is <class 'pyspark.broadcast.Broadcast'>
nameDict = spark.sparkContext.broadcast(loadMovieNames())


# **************************************
# Create schema when reading u.data
schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)])


# ***************************************
# Load up movie data as dataframe, TAB-Seperated
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///sparkdata/ml-100k/u.data")
movieCounts = moviesDF.groupBy("movieID").count()


# ****************************************
# Create a user-defined function to look up movie names from our broadcasted dictionary
# nameDict.value : The .value returns the actual dictionary record in the broadcasted variable
# You need to use ".value" on a Broadcasted object to get the actual object back.
def lookupName(movie_id):
    return nameDict.value[movie_id]


# Create a User Defined Function.
# !! Convert a Python function into a UDF that we can use in Dataframes or Spark-SQL
lookupNameUDF = func.udf(lookupName)


# Add a movieTitle column using our new udf
# - withColumn : Add a column to the DataFrame, called 'movieTitle'
# - lookupNameUDF(func.col("movieID")) : Pass the contents of the movieID column to lookupName function,
#       which returns the movieTitle.
# - movieID is known since we use DF movieCounts
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab the top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
