# Question 14: Job Code Generation, Join Decade Tagging, Active Status

# Task:
# Create job_code: name initials + year of joining + last 3 digits of salary

# Tag join decade:
# Before 2020 -> "2010s"
# 2020-2029 -> "2020s"

# active_status:
# Joined in last 2 years -> "Active"
# Else -> "Dormant"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark=SparkSession.Builder() \
    .appName("Question 14") \
    .master("local[2]") \
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
    StructField("Salary", IntegerType(), True )
])

df=spark.createDataFrame(data, schema)

# Create job_code: name initials + year of joining + last 3 digits of salary
df=df.withColumn("DOJ", to_date(col("DOJ"), "yyyy-MM-dd"))
df=df.withColumn("Job Code", concat(substring(col("Name"), 0, 1), year(col("DOJ")), regexp_extract(col("Salary"), "[0-9]{3}$", 0)))

# Tag join decade:
# Before 2020 -> "2010s"
# 2020-2029 -> "2020s"
df=df.withColumn("Join Decade", when(year(col("DOJ"))<2020, "2010s").when(year(col("DOJ"))>=2020, "2020s"))

# active_status:
# Joined in last 2 years -> "Active"
# Else -> "Dormant"
df=df.withColumn("Active_Status", when(round(datediff(current_date(), col("DOJ"))/365, 2).cast("Int")<=2, "Active").otherwise("Dormant"))

df.show()

