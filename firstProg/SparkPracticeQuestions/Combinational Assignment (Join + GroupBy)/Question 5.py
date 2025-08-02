# Read data from JSON files, join two DataFrames (customers and transactions), group by month
# using MONTH() function, and calculate the total transaction amount per month. Handle null
# transaction values with default values and write the output to Parquet format

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 5") \
    .getOrCreate()

data1=[(1, "karthik", "Premium"),
(2, "ajay", "Standard"),
(3, "vijay", "Premium"),
(4, "vinay", "Standard")]
schema1="CustomerID int, Name string, UserCategory string"

data2=[(1, "2023-06-01", 100.0),
(2, "2023-06-15", 200.0),
(1, "2023-07-01", None),
(3, "2023-07-10", 50.0)]
schema2="CustomerID int, PurchaseDate string, Amount float"

customers=spark.createDataFrame(data1, schema1)
transactions=spark.createDataFrame(data2, schema2)

joined_df=customers.join(transactions, on="CustomerID", how="outer")
df_aux=joined_df.dropna(how='any')
# df_aux.show()

avgAmount=df_aux.agg(avg(col("Amount"))).collect()[0][0]
print(avgAmount)

joined_df=joined_df.withColumn("Amount", coalesce(col("Amount"), round(lit(avgAmount), 2)))

default_date="2024-01-01"

joined_df=joined_df.withColumn("PurchaseDate", coalesce(col("PurchaseDate"), lit(default_date))) \
    .withColumn("PurchaseDate", to_date(col("PurchaseDate"), "yyyy-MM-dd")) \
    .withColumn("PurchaseMonth", month(col("PurchaseDate")))

finalDF=joined_df.groupBy("PurchaseMonth").agg(round(sum(col("Amount")), 2).alias("TotalAmount"))


finalDF.show()