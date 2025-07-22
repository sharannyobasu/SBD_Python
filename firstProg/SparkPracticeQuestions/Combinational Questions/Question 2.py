# Bonus Eligibility, Name Format, Join Anniversary Check
# Task:

# Create column bonus_eligible:
# If salary > 80000 and joined more than 2 years ago, mark "Yes", else "No"

# Extract join_year and join_month from join_date.

# Create column name_upper -> name in uppercase.
#
# Create column anniversary_month:
# If join month = current month, mark as "Yes", else "No"
#
# Use trim and lower to standardize email.
#
# Rename designation to job_title.


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

# Create column bonus_eligible:
# If salary > 80000 and joined more than 2 years ago, mark "Yes", else "No"
df=df.withColumn("Current Date", lit(current_date()))
df=df.withColumn("Experience", (round(months_between(col("Current Date"), to_date(col("join_date"), "yyyy-MM-dd"))/12)).cast("Int"))
df=df.withColumn("bonus_eligible", when((col("Salary")<80000) & (col("Experience")>=2), "Yes").otherwise("No"))

# Extract join_year and join_month from join_date.
df=df.withColumn("Join Month", date_format(col("join_date"), "MMM")) \
    .withColumn("Join Year", date_format(col("join_date"), "y"))

# Create column name_upper -> name in uppercase.
df=df.withColumn("name_upper", upper(col("name")))

# Create column anniversary_month:
df=df.withColumn("anniversary_month", when(col("Join Month")==date_format(col("Current Date"), "MMM"), "Yes").otherwise("No"))

# Use trim and lower to standardize email.
df=df.withColumn("email", lower(trim(col("email"))))

# Rename designation to job_title.
df=df.withColumnRenamed("designation", "job_title")

df.show()