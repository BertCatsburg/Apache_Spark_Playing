from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create the SparkSession
spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

# Schema for the Names
schema = StructType([
                     StructField("id", IntegerType(), True),
                     StructField("name", StringType(), True)])

# Read in the Names
names = spark.read.schema(schema).option("sep", " ").csv("file:///sparkdata/marvel/names.txt")

# The Graph data in a Dataframe
lines = spark.read.text("file:///sparkdata/marvel/graph.txt")
# lines.sort('value').show()

# Trim each line of whitespace as that could throw off the counts.
# - Add column id. Value is
connections = lines \
    .withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

