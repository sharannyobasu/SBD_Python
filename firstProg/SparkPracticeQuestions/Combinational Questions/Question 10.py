# Question 10: Join Half, Primary Email Provider, Salary Threshold Tag
# Task:
# Create join_half:
# Join month ≤ 6 → "H1"
# ≥ 6 → "H2"

# Derive email_provider from domain (lowercased)

# Flag overpaid_flag:
# If salary > 100000 and experience < 2 years → "Yes"
# Else → "No"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 10") \
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
    StructField("Salary", IntegerType(), True)]
)
df=spark.createDataFrame(data, schema)

# Task:
# Create join_half:
# Join month ≤ 6 → "H1"
# ≥ 6 → "H2"
df=df.withColumn("Join_Date", to_date(col("Join_Date"), "yyyy-MM-dd"))
df=df.withColumn("join_half", when((month(col("Join_Date"))/6)<=1, "H1").otherwise("H2"))

# Derive email_provider from domain (lowercased)
df=df.withColumn("email_provider", lower(regexp_extract(col("Email"), "@([a-zA-Z0-9]+)\.", 1)))

# Flag overpaid_flag:
# If salary > 100000 and experience < 2 years → "Yes"
# Else → "No"
df=df.withColumn("Overpaid?", when(((datediff(current_date(), col("Join_Date"))/365)<2) & (col("Salary")>100000), "Yes").otherwise("No"))

#Final DataFrame
df.show()