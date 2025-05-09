import sys
from pyspark.sql import SparkSession, functions, Window, DataFrame
import os
from pyspark import SparkConf
from pyspark.sql.functions import col, column, when, initcap, split, avg, max, sum, lag
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
os.environ["PYSPARK_PYTHON"] = "C:\python\python.exe"
#
# conf =SparkConf
# conf.set("spark.app.name",spark-program")
# conf.set("spark.master","local[*]")
spark = SparkSession.builder.appName("spark-program").getOrCreate()
data = [
     (1, "kitkat", 1000.0, "2021-01-01"),
     (2, "kitkat", 2000.0, "2021-01-02"),
     (3, "kitkat", 1000.0, "2021-01-03"),
     (4, "kitkat", 2000.0, "2021-01-04"),
     (5, "kitkat", 3000.0, "2021-01-05"),
     (6, "kitkat", 1000.0, "2021-01-06")
]
schema=["id", "name", "price", "date"]
df=spark.createDataFrame(data,schema)
df.show()