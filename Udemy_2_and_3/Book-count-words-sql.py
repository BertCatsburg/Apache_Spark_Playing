from pyspark.sql import SparkSession, functions as func

sp = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of the book into a DataFrame
inputDataFrame = sp.read.text("file:///Udemy_2_and_3/Book.txt")

# Split using a regex that extracts words
words = inputDataFrame.select(func.explode(func.split(inputDataFrame.value, "\\W+")).alias("word"))

# Take out empty strings
words.filter(words.word != "")

# Everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Grouping
wordCounts = lowercaseWords.groupBy("word").count() # Result in dataframe

# Sort by Words
wordCountSorted = wordCounts.orderBy("count", ascending=False)

wordCountSorted.show(10)

