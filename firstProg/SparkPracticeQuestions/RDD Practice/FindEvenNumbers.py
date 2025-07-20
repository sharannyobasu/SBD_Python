# Filter elements using filter.
# Create a new RDD even_numbers_rdd from numbers_rdd containing only even numbers.
# Collect and print the elements.

from pyspark import SparkContext

sc=SparkContext("local[*]", "findEvenNumbers")

data=[1,2,3,4,5,6,7,8,9]

rdd1=sc.parallelize(data)

rdd2=rdd1.filter(lambda x:x%2==0)   # finding even numbers

print(rdd2.collect())