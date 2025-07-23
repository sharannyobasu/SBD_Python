from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Assuming 'spark' is an initialized SparkSession
spark = SparkSession.builder.appName("YourApp").getOrCreate()

data = [
    ("Alice", 1, "New York", "Software Engineer", 90000),
    ("Steve", 2, "London", "Data Analyst", 75000),
    ("Charlie", 3, "Paris", "Project Manager", 100000),
    ("David", 4, "Berlin", "HR Specialist", 60000),
    ("Cristiano", 5, "New York", "Data Scientist", 95000),
    ("Politano", 6, "London", "Marketing Manager", 80000),
    (None, 7, "Tokyo", "Intern", 30000), # Example with a null name
    ("Grace", 8, None, "DevOps Engineer", 88000) # Example with a null city
]

# 3. Define the Schema (Optional but Recommended)
#    Explicitly defining the schema makes your DataFrame creation robust
#    and ensures correct data types, preventing potential inference issues.
schema = StructType([
    StructField("name", StringType(), True), # True means nullable
    StructField("id", IntegerType(), False),  # False means not nullable, id should typically not be null
    StructField("city", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# 4. Create the DataFrame
df = spark.createDataFrame(data, schema)


df.filter(col("Designation").endswith("ist") | col("Name").contains("C")).show()

df.show()