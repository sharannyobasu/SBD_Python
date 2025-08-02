# Question 5: Domain Slug, Join Quarter, Flag Active Employees
# Task:
# Extract domain and create slug version:
# Replace @gmail.com -> gmail, @yahoo.com -> yahoo, etc.
#
# Derive quarter of join (Q1-Q4) from month.
#
# Create active_flag:
# If joined in last 365 days -> "Active", else "Inactive"
#
# Format designation to title case.
#
# Rename email to email_address.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 3") \
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


# Extract domain and create slug version:
# Replace @gmail.com -> gmail, @yahoo.com -> yahoo, etc.
df=df.withColumn("domain", regexp_extract(col("email"), "(^[a-zA-Z0-9]+)([\@a-zA-Z\.]+$)" , 2)).withColumn("slug", regexp_extract(col("domain"), "@([a-zA-Z]+)", 1)).drop(col("domain")).withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))



# Derive quarter of join (Q1-Q4) from month.
df=df.withColumn("QuarterOfJoin", ceil(month(col("join_date"))/3).cast("Int"))

# Create active_flag:
# If joined in last 365 days -> "Active", else "Inactive"
df=df.withColumn("CurrentDate", lit(current_date()))
df=df.withColumn("activeflag", when(datediff(col("CurrentDate"), col("join_date")) <365, "Active").otherwise("Inactive"))

# Format designation to title case.
df=df.withColumn("designation", initcap(col("designation"))).drop("CurrentDate")

# Rename email to email_address.
df=df.withColumnRenamed("email", "email_address")
df.show()