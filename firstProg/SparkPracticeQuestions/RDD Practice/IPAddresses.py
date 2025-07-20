from pyspark import SparkContext

sc=SparkContext("local[*]", "IPAddresses")

customer_data = ['121.0.1.5 121.0.1.5 121.0.1.6 121.0.1.1 121.0.1.6 121.0.1.7 121.0.1.4 121.0.1.2 121.0.1.7 121.0.1.8 121.0.1.6 121.0.1.3 121.0.1.9 121.0.1.9 121.0.1.2 121.0.1.4']

rdd1=sc.parallelize(customer_data)

rdd2=rdd1.flatMap(lambda x:x.split(" "))
rdd3=rdd2.map(lambda x:(x, 1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)
rdd5=rdd4.map(lambda x:(x[1], x[0]))
rdd6=rdd5.sortByKey(False)
rdd7=rdd6.map(lambda x:(x[1], x[0]))
print(rdd7.collect())