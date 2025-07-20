from pyspark import SparkContext

sc=SparkContext("local[*]", "WordCount")

data=["Hello World", "Spark is Amazing", "I love Spark", "World is Amazing", "I love World"]

rdd1=sc.parallelize(data)
rdd2=rdd1.flatMap(lambda x:x.split(" "))
rdd3=rdd2.map(lambda x:(x,1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)

print(rdd4.collect())