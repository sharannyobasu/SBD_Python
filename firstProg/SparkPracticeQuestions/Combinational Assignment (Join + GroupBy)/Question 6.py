# Implement a JOIN between sales and inventory data, group by product category, and calculate
# stock availability per region using window functions. Apply WHEN and OTHERWISE to categorize
# products as "Available" or "Out of Stock"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 5") \
    .getOrCreate()

data1=[("TV", "East", 500),
("Laptop", "West", 100),
("Phone", "North", None),
("Tablet", "East", 150)]
schema1="Product string, SalesRegion string, UnitsSold int"

data2=[("TV", "East", 700),
("Laptop", "West", 50),
("Phone", "North", 300),
("Tablet", "East", None)]
schema2="Product string, InventoryRegion String, Inventory int"

df1=spark.createDataFrame(data1, schema1)
df2=spark.createDataFrame(data2, schema2)

df3=df1.join(df2, "Product", how="Outer")

df3=df3.withColumn("UnitsSold", coalesce(col("UnitsSold"), lit(0))) \
    .withColumn("Inventory", coalesce(col("Inventory"), lit(0)))

df3=df3.drop(col("SalesRegion"))
df4=df3.groupBy(["InventoryRegion", "Product"]).agg()

df3.show()