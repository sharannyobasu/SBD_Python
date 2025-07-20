# From a text RDD (e.g., from a file), find the top N longest words. If there are ties in length, any of the tied words are acceptable.
# Input: ["apple", "banana", "kiwi", "grapefruit", "orange", "strawberry"], N = 2
# Expected Output: [("grapefruit", 10), ("strawberry", 10)] (order might vary)

from pyspark import SparkContext

sc=SparkContext("local[*]", "LongestWords")

data=["apple", "banana", "kiwi", "grapefruit", "orange", "strawberry"]
n=int(input("Enter the number: "))

rdd1=sc.parallelize(data)

rdd2=rdd1.map(lambda x: (x, len(x)))
rdd3=rdd2.map(lambda x: (x[1],x[0]))
rdd4=rdd3.sortByKey(False)
rdd5=rdd4.map(lambda x:(x[1], x[0]))
print(rdd5.take(n))