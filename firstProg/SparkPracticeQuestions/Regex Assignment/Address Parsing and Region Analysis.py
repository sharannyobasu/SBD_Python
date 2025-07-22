# Problem:
# You have a DataFrame address_data with columns: address_id, full_address, city, state, and zipcode.
# Use regex_extract to extract the street number, street name, and apartment number from full_address.
# Convert the state column to uppercase.
# Filter out addresses where city starts with "New" and zipcode ends with "00".
# Group by state and calculate:
# The total count of distinct cities in each state.
# The minimum and maximum street numbers for each state.
# Drop duplicate addresses based on full_address.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .appName("AddressParsing") \
    .master("local[*]") \
    .getOrCreate()

data=[('A001', '123 Main St Apt 4B', 'New York', 'NY', '10001'),
      ('A002', '456 Elm St', 'San Francisco', 'CA', '94102'),
      ('A003', '789 Oak St Apt 12C', 'Chicago', 'IL', '60603'),
      ('A003', '790 Oak St Apt 12C', 'Kolkata', 'IL', '60603')]

schema="address_id string, full_address string, city string, state string, zipcode string"

df=spark.createDataFrame(data, schema)


# Use regex_extract to extract the street number, street name, and apartment number from full_address.
df=df.withColumn("street_num", regexp_extract(col("full_address"), "^([0-9a-zA-Z]+)\s", 1))
df=df.withColumn("street_name", regexp_extract(col("full_address"), "^[0-9]+([\sa-z-AZ0-9]+$)", 1))
df=df.withColumn("apt_name", regexp_extract(col("full_address"), "(?:Apt)\s([0-9a-zA-Z]+)$", 0))

# Convert the state column to uppercase.
df=df.withColumn("state", upper(col("state")))

# Filter out addresses where city starts with "New" and zipcode ends with "00".
df=df.filter(~(col("City").like("New%") & col("zipcode").like("%00")))

# Group by state and calculate:
# The total count of distinct cities in each state.
df_distinctCity=df.groupBy("state").agg(countDistinct(col("city")))
df_distinctCity.show()

# The minimum and maximum street numbers for each state.
df=df.withColumn("street_num", col("street_num").cast("Int"))
df_minMaxStreetNumber=df.groupBy("state").agg(min(col("Street_num")).alias("Minimum Street Num"), max(col("Street_num")).alias("Maximum Street Num"))
df_minMaxStreetNumber.show()

# Drop duplicate addresses based on full_address.
df=df.dropDuplicates(subset=["full_address"])

df.show()

