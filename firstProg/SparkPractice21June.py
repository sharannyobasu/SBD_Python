from pyspark import SparkContext

sc= SparkContext("local[*]","SparkPractice21June")

data1=[1,2,3,4,5,6]
data2=[3,4,5,6,7,8]
data3=[10,20,30]
data4=[55, 66]
data5=[("Apple", 10), ("Banana", 20), ("Banana", 90)]
data6=[("Peach", 100), ("Apple", 80), ("Banana", 77), ("Banana", 50)]

rdd1=sc.parallelize(data1)
rdd2=sc.parallelize(data2)
rdd3=sc.parallelize(data3)
rdd4=sc.parallelize(data4)
rdd5=sc.parallelize(data5)
rdd6=sc.parallelize(data6)


rdd_even=rdd1.filter(lambda x: x%2==0)
rdd_subtract=rdd1.subtract(rdd2)
rdd_union=rdd1.union(rdd2)
rdd_intersection=rdd1.intersection(rdd2)
rdd_cartesian=rdd3.cartesian(rdd4)
rdd_leftjoin=rdd5.leftOuterJoin(rdd6)

print("The various transformations are as follows:")
print("Union: ", rdd_union.collect())
print("Intersection: ", rdd_intersection.collect())
print("Subtract: ", rdd_subtract.collect())
print("Cartesian: ", rdd_cartesian.collect())
print("Even numbers: ", rdd_even.collect())
print("Left Join: ", rdd_leftjoin.collect())

output_path=r"E:\Seekho Big Data\PySpark\TestOutputDumps\tests\tests2"
rdd4.coalesce(2)
rdd4.saveAsTextFile(output_path)