# Read product data from a CSV file, calculate the sales per region using window aggregation, and
# handle missing dates with the latest available data. Write the result in ORC format.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 3") \
    .getOrCreate()

data1=[
(1, "TV", "East", 200),
(2, "Laptop", "West", None),
(3, "Phone", "North", 150),
(4, "Tablet", "East", 300)]

schema1= "Product_ID int, Product_Name string, Region string, Price int"
df1=spark.createDataFrame(data1, schema1)

data2= [(1, "East", "2023-05-01"),
(2, "West", None),
(3, "North", "2023-05-10")]

schema2= "Product_ID int, Region string, Date string"
df2=spark.createDataFrame(data2, schema2)

df1=df1.withColumn("Price", coalesce(col("Price"), lit(0)))
df2=df2.withColumn("Date", coalesce(col("Date"), lit(current_date())))

df3=df1.groupBy("Region").agg(sum(col("Price")).alias("SalesPerRegion"))

df3.write.format("ORC").mode("Overwrite").save("C:/Users/shara/PycharmProjects/workspace-sb/firstProg/SparkPracticeQuestions/SavedDF/Q3.orc")



df1.show()
df2.show()
df3.show()