# Create an RDD sentences_rdd from ["hello world", "spark is amazing", "data science"].
# Use flatMap to get an RDD words_rdd containing all individual words.
# Collect and print the elements.

from pyspark import SparkContext

sc=SparkContext("local[*]", "FlatMapTutorial")

data=["Hello World", "Spark is Amazing", "I love Spark"]

rdd1=sc.parallelize(data)
rdd2=rdd1.flatMap(lambda x:x.split(" "))


print(rdd2.collect())