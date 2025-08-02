# Question 6: Login Credentials Prep, Service Duration, Email Type Tag
# Task:

# Create login_id as first 3 letters of name (lowercased) + year of joining.

# Compute service_months using months_between.

# Tag email_type as:
# "Corporate" if domain contains company
# "Public" otherwise

# Rename designation to designation_title.


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



# Create login_id as first 3 letters of name (lowercased) + year of joining.
df=df.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))
df=df.withColumn("login_id", concat((substring(lower(col("name")), 1, 3)), (date_format(col("join_date"), "yyyy"))))

# Compute service_months using months_between.
df=df.withColumn("Service Months", months_between(current_date(), col("join_date")).cast("Int"))

# Tag email_type as:
# "Corporate" if domain contains company
# "Public" otherwise
df=df.withColumn("email_type", when(col("email").contains("company"), "Corporate").otherwise("Public"))

# Rename designation to designation_title.
df=df.withColumnRenamed("designation", "designation_title")
df.show()