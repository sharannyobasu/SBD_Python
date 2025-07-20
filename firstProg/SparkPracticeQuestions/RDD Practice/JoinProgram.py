# Join two RDDs.
# - Create product_prices_rdd from [("apple", 1.2), ("banana", 0.8), ("orange", 1.5)].
# - Join sales_data (original RDD from Question 7) with product_prices_rdd on the product name.
# - The output should be (product, (quantity, price)).
# - Collect and print the results.

# - Using the joined RDD from Question 11, calculate the total revenue for each product (quantity * price).
# - Collect and print the results.

from pyspark import SparkContext

sc=SparkContext("local[*]", "JoinProgram")

data1=[("apple", 1.2), ("banana", 0.8), ("orange", 1.5)]
data2=[("apple", 10), ("banana", 5), ("apple", 15), ("orange", 8), ("banana", 12)]

rdd1=sc.parallelize(data1)
rdd2=sc.parallelize(data2)

rdd3=rdd2.join(rdd1)    # joining the two tables

print(rdd3.collect())

rdd4=rdd3.map(lambda x: (x[0], (x[1][0]*x[1][1])))  # now calculating the total revenue
rdd5=rdd4.reduceByKey(lambda x,y: x+y)

print(rdd5.collect())