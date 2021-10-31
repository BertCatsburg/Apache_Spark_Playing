import pyspark
sc = pyspark.SparkContext('local[*]')

txt = sc.textFile('file:///sparkdisk/dups.txt')
print(txt.count())

