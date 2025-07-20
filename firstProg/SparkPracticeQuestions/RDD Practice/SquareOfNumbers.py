# Transform elements using map.

# Create a new RDD squared_numbers_rdd by squaring each number in numbers_rdd.
# Collect and print the elements.

from pyspark import SparkContext

sc=SparkContext("local[3]", 'SquareOfNumbers')

data=[1,2,3,4,5,6,7,8,9]

rdd1=sc.parallelize(data)
rdd2=rdd1.map(lambda x:x*x)

print("The squared list is: ", rdd2.collect())