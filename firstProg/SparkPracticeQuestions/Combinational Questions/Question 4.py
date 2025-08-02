# Question 4: Username Generation, Experience Bucket, Email Validation
# Task:
# Generate username by combining lowercase name + "_" + domain before .com
# Example: "karthik_gmail"
#
# Create exp_bucket column:
# <1 -> "Junior"
# 1 to <3 -> "Mid-Level"
# =3 -> "Senior"
#
# Validate email pattern:
# If email contains "@" and ends with .com, mark valid_email = "Yes", else "No"
#
# Trim extra spaces from email.
#
# Rename join_date to joining_date."

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


# Generate username by combining lowercase name + "_" + domain before .com
# Example: "karthik_gmail"
df=df.withColumn("Username", concat(lower(col("name")), lit('_'), lower(regexp_extract(col("email"), "@([a-zA-Z]+).", 1))))


# Create exp_bucket column:
# <1 -> "Junior"
# 1 to <3 -> "Mid-Level"
# =3 -> "Senior"
df=df.withColumn("Current Date", lit(current_date()))
df=df.withColumn("YoE", round(datediff(col("Current Date"), col("join_date"))/365, 2).cast("Int")).drop("Current Date")
df=df.withColumn("exp_bucket", when(col("YoE")<1, "Junior").when((col("YoE")>=1) & (col("YoE")<3), "Mid-Level").otherwise("Senior"))

# Validate email pattern:
# If email contains "@" and ends with .com, mark valid_email = "Yes", else "No"
df=df.withColumn("Valid_Email", when((col("email").contains("@")) & (col("email").contains(".com")), "Yes").otherwise("No"))

# Trim extra spaces from email.
df=df.withColumn("email", trim(col("email")))

# Rename join_date to joining_date."
df=df.withColumnRenamed("join_date", "joining_date")

final_df=df.select(col("Name"), col("Username"), col("Email"), col("valid_email"), col("joining_date"), col("YoE"), col("exp_bucket"), col("Designation"), col("Salary"))
final_df.show()