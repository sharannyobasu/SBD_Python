# Calculate the total sales per customer for different order types and apply WHEN to categorize
# customers based on sales amount (High/Medium/Low). Handle nulls for missing sales and join
# with customer info


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.Builder() \
    .appName("Question 1") \
    .master("local[*]") \
    .getOrCreate()

customers_data = [
    (1, "karthik", "2023-01-15"),
    (2, "Mohan", "2023-02-10"),
    (3, "Vinay", None)
]

customers_data_schema="ID int, Name String, Date String"

orders_data = [
    (1, "electronics", 300.50, "2023-01-20"),
    (1, "clothing", None, "2023-01-25"),
    (2, "groceries", 120.00, "2023-02-15"),
    (3, "clothing", 50.00, "2023-02-20")
]

orders_data_schema="ID int, Category string, saleAmount double, saleDate string"

df1=spark.createDataFrame(customers_data, customers_data_schema)
df2=spark.createDataFrame(orders_data, orders_data_schema)

df1=df1.withColumn("date", when(col("date").isNull(), "2023-01-01").otherwise(col("date")))
df2=df2.withColumn("saleAmount", when(col("saleAmount").isNull(), 0).otherwise(col("saleAmount")))

df3=df1.join(df2, on="ID", how="Inner")
df4=df3.groupBy(["Name", "Category"]).agg(sum(col("saleAmount")).alias("TotalSalesPerCustomer")).sort(col("TotalSalesPerCustomer").desc())
df4=df4.withColumn("SaleCategory", when(col("TotalSalesPerCustomer")<100, "Low").when((col("TotalSalesPerCustomer")>=100) & (col("TotalSalesPerCustomer")<300), "Medium").otherwise("High"))

# df1.show()
# df2.show()
df3.show()
df4.show()