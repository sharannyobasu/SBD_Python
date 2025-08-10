# Question 15: Professional Title Formatting, Corporate Mail Check, Compensation Index
#
# Task:
# Format designation to snake_case (data_engineer)

# Check if email is corporate (endsWith("@company.org"))

# Compensation Index:
# salary / months_since_joining -> store as comp_index with 2 decimals

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 15") \
    .getOrCreate()

data = [
    ("Karthik", "karthik@gmail.com", "2023-12-12", "Data Engineer", 85000),
    ("Veer", "veer@yahoo.com", "2022-11-01", "Business Analyst", 65000),
    ("Veena", "veena@company.org", "2021-06-15", "Data Scientist", 105000),
    ("Vinay", "vinay@gmail.com", "2024-01-10", "Intern", 25000),
    ("Vijay", "vijay@hotmail.com", "2020-05-22", "Senior Manager", 125000)
]

schema=StructType([
    StructField("Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("DOJ", StringType(), True),
    StructField("Designation", StringType(), True),
    StructField("Salary", IntegerType(), True)
])

df=spark.createDataFrame(data, schema)

# Format designation to snake_case (data_engineer)
df=df.withColumn("Designation", lower(regexp_replace(col("Designation"), "[\s]", '_')))

# Check if email is corporate (endsWith("@company.org"))
df=df.withColumn("Is Corporate?", when(col("Email").endswith('@company.org'), "Yes").otherwise("No"))

# Compensation Index:
# salary / months_since_joining -> store as comp_index with 2 decimals
df=df.withColumn("DOJ", to_date(col("DOJ"), "yyyy-MM-dd")) \
    .withColumn("comp_index", round(col("Salary")/months_between(current_date(), col("DOJ")), 2))


df.show()