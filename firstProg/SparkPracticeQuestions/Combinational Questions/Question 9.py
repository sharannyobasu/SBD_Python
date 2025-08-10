#Question 9: Weekend Joiner Tag, Domain Popularity, Tenure Title

# Task:

# Derive day_joined (Monday to Sunday)

# Create weekend_joiner flag if joined on Saturday or Sunday

# Check if email domain is one of popular domains: ["gmail", "yahoo", "hotmail"]

# Based on tenure:

# <1 year -> "Fresher"
# 1-3 years -> "Stable"
# 3 years -> "Long-Term"


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

df=df.withColumn("day_joined", date_format(col("join_date"), 'EEEE'))
df=df.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))


# Create weekend_joiner flag if joined on Saturday or Sunday
df=df.withColumn("weekendJoiner", when((col("day_joined")=='Sunday') | (col("day_joined")=='Saturday'), "Yes").otherwise("No"))


# Check if email domain is one of popular domains: ["gmail", "yahoo", "hotmail"]
df=df.withColumn("domain", regexp_extract(col("email"), "@([a-zA-Z0-9]+)\.", 1))
df=df.withColumn("registeredDomains", when(col("domain").isin(["gmail", "yahoo", "hotmail"]), "Yes").otherwise("No")).drop(col("domain"))


# Based on tenure:
# <1 year -> "Fresher"
# 1-3 years -> "Stable"
# 3 years -> "Long-Term"
df=df.withColumn("tenure", when((datediff(current_date(), col("join_date"))/365)<1, "Fresher").when(((datediff(current_date(), col("join_date"))/365)>=1.00) & ((datediff(current_date(), col("join_date"))/365)<3.00), "Stable").otherwise("Long-Term"))

df.show()