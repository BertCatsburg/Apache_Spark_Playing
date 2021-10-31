from pyspark.sql import SparkSession

sp = SparkSession.builder.appName('SparkSQL').getOrCreate()

people = sp\
    .read\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .csv("file:///Udemy_2_and_3/friends-by-age-with-header.csv")

print ("Our Infered Schema")
people.printSchema()

people.select("name").show()

people.filter(people.age < 21).show()

people.groupBy("age").count().orderBy("age").show()

sp.stop()
