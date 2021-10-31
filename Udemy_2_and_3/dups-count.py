import pyspark
sc = pyspark.SparkContext('local[*]')

txt = sc.textFile('file:///Udemy_2_and_3/dups.txt')
print(txt.count())

