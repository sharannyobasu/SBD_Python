# Sort a Pair RDD by key.
#
# Sort the sales_data RDD (after reduceByKey) by product name in ascending order.
# Collect and print the results.

from pyspark import SparkContext

sc=SparkContext("local[5]", "SortPairRDD")

data=[("apple", 10), ("banana", 5), ("apple", 15), ("orange", 8), ("banana", 12)]

rdd1=sc.parallelize(data)

rdd2=rdd1.reduceByKey(lambda x, y: x+y ).sortByKey(True)   # alphabetical order if True, remember this sorts by key not by value
print(rdd2.collect())