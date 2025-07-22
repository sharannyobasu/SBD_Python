# Question 1: Performance Banding, Experience Category, and Domain Detection

# Task:
# From the employee data, do the following:
# Derive email_domain by extracting everything after "@" and converting it to lowercase.

# Create a new column experience_years using months_between and round it to nearest integer.

# Based on experience:
# < 1 year → "New"
# 1 to < 3 → "Experienced"
# ≥ 3 → "Veteran"

# Create a new column salary_band:
# < 50000 → "Low"
# 50000 to 100000 → "Medium"
# 100000 → "High"

# Mark is_internal as "Yes" if email ends with company.org, else "No".

# Rename salary to current_salary.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 1") \
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


# Derive email_domain by extracting everything after "@" and converting it to lowercase.
df=df.withColumn("email_domain", lower(regexp_extract(col("email"), "@([a-zA-Z\.]+$)", 1)))

# Create a new column experience_years using months_between and round it to nearest integer.
df=df.withColumn("Current Date", lit(current_date()))
df=df.withColumn("experience_years", round(round(months_between(col("Current Date"), col("join_date"))).cast("Int")/12).cast("Int"))



# Based on experience:
# < 1 year → "New"
# 1 to < 3 → "Experienced"
# ≥ 3 → "Veteran"

df=df.withColumn("experience",  when(col("experience_years")<1, "New") \
                 .when((col("experience_years")>=1) & (col("experience_years")<3), "Experienced") \
                 .otherwise("Veteran"))

# Create a new column salary_band:
# < 50000 → "Low"
# 50000 to 100000 → "Medium"
# 100000 → "High"
df=df.withColumn("Salary Band", when(col("salary")<50000, "Low") \
                 .when((col("Salary")>=50000) & (col("Salary")<100000), "Medium") \
                 .otherwise("High"))

# Mark is_internal as "Yes" if email ends with company.org, else "No".
df=df.withColumn("Is_Internal?", when(col("email_domain").contains("org"), "Yes").otherwise("No"))

# Rename salary to current_salary.
df=df.withColumnRenamed("Salary", "Current_Salary")

df.show()