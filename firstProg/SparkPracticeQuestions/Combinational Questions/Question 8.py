# Question 8: Joining Cohort, Email Brand Match, Name Formalization

# Task:

# Create join_cohort:
# Joined before 2022 -> "Old Batch"
# 2022-2023 -> "Mid Batch"
# After 2023 -> "New Batch"
#
# Tag brand_matched_email:
# If name part is present in email -> "Matched"
# Else -> "Unmatched"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import datediff

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 6") \
    .getOrCreate()


data = [
    ("Karthik", "karthik@gmail.com", "2023-12-12", "Data Engineer", 85000),
    ("Veer", "veer@yahoo.com", "2022-11-01", "Business Analyst", 65000),
    ("Veena", "veena@company.org", "2021-06-15", "Data Scientist", 105000),
    ("Vinay", "vinay@gmail.com", "2024-01-10", "Intern", 25000),
    ("Vijay", "vijay@hotmail.com", "2020-05-22", "Senior Manager", 125000)
]

schema="name string, email string, join_date string, designation string, salary int"

df=spark.createDataFrame(data, schema)

# Create join_cohort:
# Joined before 2022 -> "Old Batch"
# 2022-2023 -> "Mid Batch"
# After 2023 -> "New Batch"

df=df.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))
df=df.withColumn("Cohort", when(year(col("join_date"))<2022, "Old Batch").when((year(col("join_date"))>=2022) & (year(col("join_date"))<=2023), "Mid Batch").otherwise("New Batch"))

# Tag brand_matched_email:
# If name part is present in email -> "Matched"
# Else -> "Unmatched"
df=df.withColumn("brand_matched_email", when(col("email").contains(lower(col("name"))), "Matched").otherwise("Unmatched"))
df.show()