# Question 11: Gmail Tagging, Date Rank, Multi-Condition Badge

# Task:

# Flag is_gmail_user

# Create seniority_score:
# Base: 10 points if Senior, +5 if > 2 years, +5 if salary > 90000

# Create badge:
# Score ≥ 20 → "Gold"
# 15–19 → "Silver"
# Else → "Bronze"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 11") \
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
    StructField("Join_Date", StringType(), True),
    StructField("Designation", StringType(), True),
    StructField("Salary", IntegerType(), True)
])



df=spark.createDataFrame(data, schema)

df=df.withColumn("GmailUser?", when(col("email").contains("gmail.com"), "Yes").otherwise("No"))
df=df.withColumn("Join_Date", to_date(col("Join_Date"), "yyyy-MM-dd"))


# Create seniority_score:
# Base: 10 points if Senior, +5 if > 2 years, +5 if salary > 90000
df=df.withColumn("Score", lit(0).cast("Int"))
df=df.withColumn("Score", (when(col("Designation").contains("Senior"), 10).otherwise(0))  + \
                           (when(((datediff(current_date(), col("Join_Date"))/365)>2), 5).otherwise(0)) + \
                           (when(col("salary")>90000, 5).otherwise(0)))

# Create badge:
# Score ≥ 20 → "Gold"
# 15–19 → "Silver"
# Else → "Bronze"
df=df.withColumn("badge", when(col("Score")>=20, 'Gold').when((col("Score")>=15) & (col("Score")<=19), 'Silver').otherwise("Bronze"))

df.show()