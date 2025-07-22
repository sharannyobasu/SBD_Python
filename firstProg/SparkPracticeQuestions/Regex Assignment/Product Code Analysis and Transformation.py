# Problem:
# You have a DataFrame product_data with columns: product_id, product_code, release_date, and category.
# Split the product_code into three separate columns: brand_code, category_code, and serial_number.
# Convert the category column to title case using initcap.
# Filter out products where brand_code starts with "X" and serial_number ends with "99".
# Group by category_code and calculate:
# The sum of serial numbers per category.
# The count of distinct brand_codes per category.
# Drop duplicates based on product_code.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .appName("Problem 3") \
    .master("local[6]") \
    .getOrCreate()

data= [('P001', 'A123-456-789', '2024-04-01', 'electronics'), \
       ('P002', 'B234-567-891', '2024-05-10', 'home_appliance'), \
       ('P003', 'X345-678-999', '2024-06-15', 'furniture')]

schema="Product_ID String, Product_Code String, Release_Date String, Category String"
df=spark.createDataFrame(data, schema)
df=df.withColumn("Release_Date", to_date(col("Release_Date"),"yyyy-MM-dd"))

# Split the product_code into three separate columns: brand_code, category_code, and serial_number.
df=df.withColumn("Brand_Code", regexp_extract(col("Product_Code"), "^[A-Za-z0-9]+", 0)) \
    .withColumn("Category_Code", regexp_extract(col("Product_Code"), "-([0-9]+)-", 1)) \
    .withColumn("Serial_Number", regexp_extract(col("Product_Code"), "[0-9]+$", 0).cast("Int"))

# Convert the category column to title case using initcap.
df=df.withColumn("Category", initcap(col("Category")))

# Filter out products where brand_code starts with "X" and serial_number ends with "99".
df=df.filter(~(col("Brand_Code").rlike("^X") & col("Serial_Number").cast("String").rlike("99$")))
df.show()

# Group by category_code and calculate:
# The sum of serial numbers per category.

df_sumSerial=df.groupby("Category_Code").agg(sum(col("Serial_Number")).alias("SumSerialCode_PerCategory"))
df_sumSerial.show()

# The count of distinct brand_codes per category.
df_BrandCodes_PerCategory=df.groupBy("Category_Code").agg(countDistinct(col("Brand_Code")).alias("BrandCodes_PerCategory"))
df_BrandCodes_PerCategory.show()


# Drop duplicates based on product_code.
df_droppedDup=df.dropDuplicates(subset=["Product_Code"])
df_droppedDup.show()


df.show()