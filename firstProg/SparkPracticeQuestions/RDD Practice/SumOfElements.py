# Perform a basic reduction using reduce.
# Calculate the sum of all elements in numbers_rdd

from pyspark import SparkContext
sc=SparkContext("local[*]", "SumOfElements")

data=[1,2,3,4,5,6,7,8,9]

rdd=sc.parallelize(data)
sum=rdd.reduce(lambda x, y:x+y)
# since reduce returns only 1 number, it returns an integer and not an RDD in variable sum.
# Data type of sum is integer, it is not an RDD actually.

print("Sum of elements: ", sum)