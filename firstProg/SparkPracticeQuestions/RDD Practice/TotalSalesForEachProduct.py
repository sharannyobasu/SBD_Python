# Create a Pair RDD and use reduceByKey.

# Create an RDD sales_data from [("apple", 10), ("banana", 5), ("apple", 15), ("orange", 8), ("banana", 12)].
# Calculate the total sales for each product using reduceByKey.
# Collect and print the results.

from pyspark import SparkContext

sc=SparkContext("local[7]", "TotalSalesForEachProduct")

sales_data=[("apple", 10), ("banana", 5), ("apple", 15), ("orange", 8), ("banana", 12)]

rdd1=sc.parallelize(sales_data)
rdd2=rdd1.reduceByKey(lambda x, y: x+y)
# print("Number of partitions: ", rdd2.getNumPartitions())


for i in rdd2.collect():
    print("Total Sales for", i[0], "is", i[1])