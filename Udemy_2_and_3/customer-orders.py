import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

_input = sc.textFile("file:///Udemy_2_and_3/customer-orders.csv")

# Split the program into fields, seperate on comma
custAmount01 = _input.map(lambda x: x.split(','))

# Only take CustomerID and Amount. (Remove ItemId)
custAmount02 = custAmount01.map(lambda x: (int(x[0]), float(x[2])))

# In the tuple (customerId, Amount), add the Amounts
custAmount03 = custAmount02.reduceByKey(lambda x, y: x + y)
for x in custAmount03.collect():
    if x[0] == 44:
        print(f"*** {x}")

# Flip Keys and Values, and sort the values
custAmount04 = custAmount03.map(lambda x: (x[1], x[0])).sortByKey()

for i in custAmount04.collect():
    print(f"Customer {i[1]} spent {round(i[0]*100) / 100}")
