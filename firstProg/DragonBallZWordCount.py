from pyspark import SparkContext

sc= SparkContext("local[*]","WordCount")
rdd1=sc.textFile("E://Seekho Big Data/DragonBallZ.txt")
print(rdd1.getNumPartitions())
rdd2=rdd1.flatMap(lambda x:x.split(" "))
rdd3=rdd2.map(lambda x:(x,1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)
for i in rdd4.sortByKey().collect():
    print(i)
