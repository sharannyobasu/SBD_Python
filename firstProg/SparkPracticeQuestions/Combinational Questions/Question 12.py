# Question 12: Senior Hiring Year Check, Domain Slugify, Tag Designation Source

# Task:

# If designation.startsWith("Senior") and joined before 2021 -> "Legacy Hire"

# Slugify email domain (e.g., gmail.com -> gmail)

# Mark designation_source as:
# "Technical" if designation contains "Data"
# "Business" if contains "Analyst"
# "Leadership" otherwise

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 12") \
    .getOrCreate()

data = [
    ("Karthik", "karthik@gmail.com", "2023-12-12", "Data Engineer", 85000),
    ("Veer", "veer@yahoo.com", "2022-11-01", "Business Analyst", 65000),
    ("Veena", "veena@company.org", "2021-06-15", "Data Scientist", 105000),
    ("Vinay", "vinay@gmail.com", "2024-01-10", "Intern", 25000),
    ("Vijay", "vijay@hotmail.com", "2020-05-22", "Senior Manager", 125000),
    ("Khalid", "khalidy@orkut.com", "2022-08-22", "Data Analyst", 45000)
]

schema=StructType([
    StructField("Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("DOJ", StringType(), True),
    StructField("Designation", StringType(), True),
    StructField("Salary", IntegerType(), True)]
)

df=spark.createDataFrame(data, schema)

# If designation.startsWith("Senior") and joined before 2021 -> "Legacy Hire"
df=df.withColumn("DOJ", to_date(col("DOJ"), "yyyy-MM-dd"))
df=df.withColumn('EmployeeType', when((col("Designation").contains("Senior")) & (year("DOJ")<2021), "Legacy-Hire").otherwise("Non-Legacy"))

# Slugify email domain (e.g., gmail.com -> gmail)
df=df.withColumn("EmailSlug", regexp_extract(col("Email"), "@([a-zA-Z0-9]+).", 1))

# Mark designation_source as:
# "Technical" if designation contains "Data"
# "Business" if contains "Analyst"
# "Leadership" otherwise
df=df.withColumn("Designation_Source", when(col("Designation").contains("Data"), "Technical") \
                 .when(col("Designation").contains("Analyst"), "Business").otherwise("Leadership"))

df=df.show()

