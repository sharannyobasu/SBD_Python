# Question 13: Intern Exit Planning, Domain Validity, Name Check

# Task:
# If Intern and join_date < 6 months -> "Extend Probation"

# Mark email_valid if domain is in approved list: gmail.com, company.org

# Tag name_check:
# If name contains 'a' or starts with 'v' -> "Pattern Hit"
# Else -> "Safe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 13") \
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
    StructField("DOJ", StringType(), True),
    StructField("Designation", StringType(), True),
    StructField("Salary", IntegerType(), True)
])

df=spark.createDataFrame(data, schema)

# If Intern and join_date < 6 months -> "Extend Probation"
df=df.withColumn("DOJ", to_date(col("DOJ"), "yyyy-MM-dd"))
df=df.withColumn("Probation_Status", when((col("Designation").contains("Intern")) & (datediff(current_date(),col("DOJ"))<180), "Extend Probation") \
                 .otherwise("N/A"))

df=df.withColumn("domain", regexp_extract(col("Email"), "@([a-zA-Z0-9\.]+$)", 1)) \
    .withColumn("Valid Email", when(col("domain").isin(["gmail.com", "company.org"]), "Valid").otherwise("Invalid")).drop("domain")

# Tag name_check:
# If name contains 'a' or starts with 'v' -> "Pattern Hit"
# Else -> "Safe"
df=df.withColumn("name_check", when((lower(col("Name")).startswith('v')) | (lower(col("Name")).contains('a')) ,"Pattern Hit" ).otherwise("Safe"))

df.show()


