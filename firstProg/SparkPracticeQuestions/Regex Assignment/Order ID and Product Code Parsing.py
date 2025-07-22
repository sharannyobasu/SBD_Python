# Problem:
# You have a DataFrame order_data with columns: order_id, product_code, order_date, and delivery_status.
# Use substring to extract the first 3 characters of order_id and the last 4 characters of product_code.
# Convert the delivery_status to lowercase.
# Filter out orders where order_id starts with "ORD" AND delivery_status ends with "delivered".
# Group by delivery_status and calculate:
# The sum of the numeric part of the order_id.
# Drop duplicate orders based on order_id.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .appName("Problem 7") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ('ORD001', 'P123-4567', '2024-08-01', 'Delivered'),
    ('ORD002', 'P234-5678', '2024-08-05', 'Pending'),
    ('ORD003', 'P345-6789', '2024-08-10', 'Delivered')
]

schema="order_id string, product_code string, date_deliver string, delivery_status string"

df=spark.createDataFrame(data, schema)

# Use substring to extract the first 3 characters of order_id and the last 4 characters of product_code.
df=df.withColumn("Order_id_last3", substring(col("order_id"), 0, 3)) \
    .withColumn("ProductCode_Last4", substring(col("product_code"), -4, 4))

# Convert the delivery_status to lowercase.
df=df.withColumn("delivery_status", lower(col("delivery_status")))

# Filter out orders where order_id starts with "ORD" AND delivery_status ends with "delivered".
#df=df.filter(~((col("order_id").like("ORD%")) & (col("delivery_status").contains("delivered"))))

# Group by delivery_status and calculate:
# The sum of the numeric part of the order_id.
df=df.withColumn("OrdNum", substring(col("order_id"), 4, 100000).cast("Int"))
df_sumNumeric=df.groupBy("delivery_status").agg(sum(col("ordNum")).alias("Sum of OrdNum"))
df_sumNumeric.show()


# Drop duplicate orders based on order_id.
df=df.dropDuplicates(subset=["order_id"])

df.show()