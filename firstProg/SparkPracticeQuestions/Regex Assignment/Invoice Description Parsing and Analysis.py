# Problem:
# You have a DataFrame invoice_data with columns: invoice_id, description, quantity, unit_price, and total_amount.
# Split the description into product_name, color, and size using split.
# Convert the product_name to uppercase.
# Filter out invoices where the quantity is less than 5 AND the total_amount is greater than 1000.
# Group by color and size and calculate:
# The total quantity sold.
# The average unit_price for each color-size combination.
# Drop duplicate invoices based on description.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .appName("Problem 6") \
    .master("local[7]") \
    .getOrCreate()

data=[(1001, 'T-shirt Red Large', 10, 20, 200),(1002, 'Jeans Blue Medium', 3, 50, 150), (1003, 'Jacket Black Small', 5, 100, 500), (1004, 'Jacket Black Small', 4, 900, 3600)]

schema="Invoice_Id int, Description string, quantity int, unit_price int, total_amount int"

df=spark.createDataFrame(data, schema)

# Split the description into product_name, color, and size using split.
df=df.withColumn("Product_Name", split(col("Description"), " ").getItem(0)) \
    .withColumn("Color", split(col("Description"), " ").getItem(1)) \
    .withColumn("Size", split(col("Description"), " ").getItem(2))

# Convert the product_name to uppercase.
df=df.withColumn("Product_Name", upper(col("Product_Name")))

# Filter out invoices where the quantity is less than 5 AND the total_amount is greater than 1000.
df=df.filter(~((col("quantity")< 5) & (col("total_amount")> 1000)))

# Group by color and size and calculate:
# The total quantity sold.
df_quantitySold=df.groupBy("color").agg(sum(col("quantity")).alias("TotalQuantitySold"))
df_quantitySold.show()

# The average unit_price for each color-size combination.
df_avgUnitPrice=df.groupBy(["color", "size"]).agg(avg(col("Unit_Price")).alias("Avg_UnitPrice"))
df_avgUnitPrice.show()


# Drop duplicate invoices based on description.
df=df.dropDuplicates(subset=["Description"])


df.show()