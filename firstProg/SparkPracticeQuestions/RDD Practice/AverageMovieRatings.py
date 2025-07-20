from pyspark import SparkContext

sc=SparkContext("local[*]", "AvgMovieRatings")

ratings_rdd = sc.parallelize([(1, 101, 4.0, 123), (1, 102, 3.0, 124), (2, 101, 5.0, 125),
                              (2, 103, 2.0, 126), (1, 101, 3.0, 127)])

rdd1=ratings_rdd.map(lambda x: (x[1], (x[2], 1)))
rdd2=rdd1.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

rdd3=rdd2.map(lambda x: (x[0], x[1][0]/x[1][1]))
print(rdd3.collect())
