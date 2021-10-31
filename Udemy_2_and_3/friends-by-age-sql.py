from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a Spark Session
sparkSess = SparkSession.builder.appName('SparkSQL').getOrCreate()


# Function to map line to SQL-Row
def mapper(line):
    fields = line.split(',')
    return Row(
        ID=int(fields[0]),
        name=str(fields[1].encode("utf-8")),
        age=int(fields[2]),
        numFriends=int(fields[3])
    )


# This creates an RDD (because of SparkContext)
lines = sparkSess.sparkContext.textFile('file:///Udemy_2_and_3/friends-by-age.csv')
# Create a Row of this Line
people = lines.map(mapper)

# Convert from RDD to a DataFrame
schemaPeople = sparkSess.createDataFrame(people).cache() # Cache: Keep it in memory
# Create a Temp View to be able to use the DataFrame as a Database Table
schemaPeople.createOrReplaceTempView("people") # "people" is the Viewname

# Results of SQL Queries are RDD's
teenagers = sparkSess.sql("SELECT * FROM people WHERE age >= 13 AND age <= 25 ORDER BY numFriends DESC")

# Show results
# for teen in teenagers.collect()[:20]:
#     print(f"Teen is {teen}")
teenagers.show()

# Close the database
sparkSess.stop()
