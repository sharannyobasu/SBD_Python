# QUESTION 1:

# val employees = List(
#   (1, "AJAY", 28),
#   (2, "VIJAY", 35),
#   (3, "MANOJ", 22)
# ).toDF("id", "name", "age")
#
# Question: How would you add a new column is_adult which is true if the age is greater than or equal to 18, and false otherwise?


# SOLUTION:


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
#
# spark=SparkSession.Builder() \
#     .appName("DFPractice") \
#     .master("local[*]") \
#     .getOrCreate()
#
# data=[(1, 'Ajay', 28), (2, 'Vijay', 35), (3, 'Manoj', 22)]
# schema="id int, name string, age int"
#
# employee_df=spark.createDataFrame(data, schema)
#
# result_df=employee_df.withColumn("Adult/Child", when(col("age")>=18, "Adult").otherwise("Child"))
# result_df.show()

############################################################################################################################################################
############################################################################################################################################################
############################################################################################################################################################

# QUESTION 2:

# val grades = List(
#   (1, 85),
#   (2, 42),
#   (3, 73)
# ).toDF("student_id", "score")
# Question: How would you add a new column grade with values "Pass" if score is greater than or equal to 50,
# and "Fail" otherwise?

# SOLUTION:

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
#
# spark=SparkSession.Builder() \
#     .appName("Grades") \
#     .master("local[*]") \
#     .getOrCreate()
#
# data2=[(1, 85), (2, 42), (3, 73)]
# grades=spark.createDataFrame(data2, "id int, score int")
#
# result_df=grades.withColumn("Verdict", when(col("score")>=50,"Pass").otherwise("Fail"))
# result_df.show()


############################################################################################################################################################
############################################################################################################################################################
############################################################################################################################################################


# QUESTION 3:

# val transactions = List(
#   (1, 1000),
#   (2, 200),
#   (3, 5000)
# ).toDF("transaction_id", "amount")
#
# Question: How would you add a new column category with values "High" if amount is greater than 1000,
# "Medium" if amount is between 500 and 1000, and "Low" otherwise?

# SOLUTION:

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# spark=SparkSession.Builder() \
#     .appName("Transactions") \
#     .master("local[*]") \
#     .getOrCreate()
#
# data=[(1, 2000), (2, 200), (3, 5000), (4, 800)]
# schema='id int, amount int'
#
# transactions=spark.createDataFrame(data,schema)
#
# result_df=transactions.withColumn('category', when(col('amount')>1000, "High").when(col("amount")>500, "Medium").otherwise("Low"))
# result_df.show()


############################################################################################################################################################
############################################################################################################################################################
############################################################################################################################################################


# QUESTION 4:


# val products = List(
#   (1, 30.5),
#   (2, 150.75),
#   (3, 75.25)
# ).toDF("product_id", "price")
# Question: How would you add a new column price_range with values "Cheap" if price is less than 50,
# "Moderate" if price is between 50 and 100, and "Expensive" otherwise?

# SOLUTION:

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
#
# spark=SparkSession.Builder() \
#     .appName("Products") \
#     .master("local[*]") \
#     .getOrCreate()
#
# data=[(1, 30.5), (2, 150.75), (3, 75.25), (4,93.23)]
# schema1=StructType([
#     StructField("id", IntegerType(), True),
#     StructField("price", DoubleType(), True)
# ])
#
# products=spark.createDataFrame(data, schema=schema1)
#
# result_df=products.withColumn("Price Range", when(col("price")>100.0, "Expensive").when((col("price")>50.0) & (col("price")<=100.0), "Moderate").otherwise("Cheap"))
# result_df.show()

############################################################################################################################################################
############################################################################################################################################################
############################################################################################################################################################


# QUESTION 5:

# val events = List(
#   (1, "2024-07-27"),
#   (2, "2024-12-25"),
#   (3, "2025-01-01")
# ).toDF("event_id", "date")
# Question: How would you add a new column is_holiday which is true if the date is "2024-12-25" or "2025-01-01",
# and false otherwise?

# SOLUTION:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("Holiday") \
    .master("local[*]") \
    .getOrCreate()

# Keep dates as strings
data = [(1, "2024-07-27"), (2, "2024-12-25"), (3, "2025-01-01")]

# Use StringType for initial schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("date", StringType(), True)
])

# Create DataFrame
holiday = spark.createDataFrame(data, schema)

# Convert to proper DateType
holiday = holiday.withColumn("date", to_date("date", "yyyy-MM-dd"))

# Add isHoliday column
result_df = holiday.withColumn(
    "isHoliday", when((col("date") == lit("2024-12-25").cast("date")) | (col("date") == lit("2025-01-01").cast("date")),"True").otherwise("False"))

result_df.show()