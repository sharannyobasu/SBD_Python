# Count elements in an RDD.

# Using numbers_rdd, find the total number of elements.

from pyspark import SparkContext

sc=SparkContext("local[*]", "CountOfElements")

data=[1,2,3,4,5,6,7,8, 9, 10]
rdd1=sc.parallelize(data)
countOfElements=rdd1.count()

print("Total number of elements in this RDD is: ", countOfElements)