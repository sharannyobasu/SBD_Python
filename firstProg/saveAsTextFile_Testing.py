from pyspark import SparkContext

sc= SparkContext("local[*]","SparkPractice21June")

data1= list(range(1, 101))
rdd1=sc.parallelize(data1)
rdd2=rdd1.map(lambda x:x+12)

output_path=r"E:\Seekho Big Data\PySpark\TestOutputDumps\tests\tests4"
rdd3=rdd2.repartition(2)
rdd3.saveAsTextFile(output_path)

print(rdd3.collect())