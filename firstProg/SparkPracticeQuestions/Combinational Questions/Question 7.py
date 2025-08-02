# Question 7: Identify Attrition Risk, Normalize Email, Role Banding

# Task:

# Create attrition_risk:
# If joined more than 4 years ago and salary < 60000 -> "High"
# Else if < 2 years -> "Low"
# Else -> "Medium"

# Standardize email using trim + lower

# Tag role_band:
# designation.startsWith("Senior") -> "L3"
# designation.contains("Analyst") -> "L2"
# Else -> "L1""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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

# Create attrition_risk:
# If joined more than 4 years ago and salary < 60000 -> "High"
# Else if < 2 years -> "Low"
# Else -> "Medium"
df=df.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd")) \
    .withColumn("YoE", (datediff(current_date(), col("join_date"))/365).cast("Int"))

df=df.withColumn("attrition_risk", when((col("YoE")>4) & (col("Salary")<60000), "High").when(col("YoE")<2, "Low").otherwise("Medium"))

# Standardize email using trim + lower
df=df.withColumn("email", trim(lower(col("email"))))


# Tag role_band:
# designation.startsWith("Senior") -> "L3"
# designation.contains("Analyst") -> "L2"
# Else -> "L1""
df=df.withColumn("Role_Band", when(col("designation").contains("Senior"), "L3").when(col("designation").contains("Analyst"), "L2").otherwise("L1"))


df.show()