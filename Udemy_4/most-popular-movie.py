from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

# Init Spark
sp = SparkSession.builder.appName("MostPopularMovie").getOrCreate()

# Define the Schema
movieSchema = StructType([
    StructField(name="UserId", dataType=IntegerType(), nullable=True),
    StructField(name="MovieId", dataType=IntegerType(), nullable=True),
    StructField(name="Rating", dataType=IntegerType(), nullable=True),
    StructField(name="Timestamp", dataType=LongType(), nullable=True)
])

# Load the data in a DataFrame
moviesDF = sp.read.option("sep", "\t").schema(movieSchema).csv("file:///sparkdata/ml-100k/u.data")

# Group and count and show first 10
topMovieIds = moviesDF.groupBy("MovieId").count().orderBy(func.desc("count"))
topMovieIds.show(10)

sp.stop()