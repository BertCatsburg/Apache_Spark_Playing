from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql import functions as func

sp = SparkSession.builder.appName('CustomerOrders').getOrCreate()

# Define the Schema
schema = StructType([
    StructField("CustId", IntegerType(), True),
    StructField("ItemId", IntegerType(), True),
    StructField("Amount", FloatType(), True)])

# Read the file as dataframe
df = sp.read.schema(schema).csv("file:///Udemy_2_and_3/customer-orders.csv")

# Select only stationID and temperature
RelevantFields = df.select("CustId", "Amount")
# RelevantFields.show()

AggCustomers = RelevantFields\
    .groupBy("CustId")\
    .agg(func.round(func.sum("Amount"), 2).alias("TotalSpend"))\
    .sort("TotalSpend", ascending=False)

AggCustomers.show()

sp.stop()