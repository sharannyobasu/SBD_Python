# Problem:
# You have a DataFrame phone_data with columns: customer_id, phone_number, signup_date, and region.
# Use regex_extract to extract the country code, area code, and local number from the phone_number column.
# Convert the region column to lowercase.
# Filter customers whose country code is not +1 (USA).
# Group by region and calculate:
# The count of customers per region.
# The most frequent area code within each region.
# Drop rows where the phone number is duplicated.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Problem 2"). \
    getOrCreate()

data=[('C001', '+1-415-5551234', '2024-01-10', 'WEST'), ('C002', '+44-20-79461234', '2024-02-20', 'EAST'), ('C003', '+91-22-23451234', '2024-03-15', 'NORTH')]

df=spark.createDataFrame(data, 'Customer_ID String, Phone_Number String, Signup_Date String, Region String')
df=df.withColumn("Signup_date", to_date(col("Signup_date"), "yyyy-MM-dd"))
df.show()

# Use regex_extract to extract the country code, area code, and local number from the phone_number column.
df=df.withColumn("Country_Code", regexp_extract(col("Phone_Number"),"^[\+?0-9]+", 0)) \
    .withColumn("Area_Code", regexp_extract(col("Phone_Number"), "-([0-9]+)-", 1)) \
    .withColumn("Phone Number", regexp_extract(col("Phone_Number"),"[0-9]+$" , 0))
df.show()

# Convert the region column to lowercase.
df=df.withColumn("Region", lower(col("Region")))
df.show()

# Filter customers whose country code is not +1 (USA).
df_non_USA=df.filter(col("Country_Code")!='+91')
df_non_USA.show()

# Group by region and calculate:
# The count of customers per region.
# The most frequent area code within each region.

df_Count_Per_Region=df.groupby(col("Region")).agg(count(col("Customer_Id")).alias("Count_Per_Region"))
df_Count_Per_Region.show()
df_Freq_Area_Code=df.groupby(["Region", "Area_Code"]).agg(count("Area_Code").alias("Area_Codes_By_Region"))
df_Freq_Area_Code.show()

# Drop rows where the phone number is duplicated.
df=df.dropDuplicates(subset=["Phone Number"])
df.show()


