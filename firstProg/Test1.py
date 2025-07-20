import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import shutil

# Set Hadoop environment
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

# Start Spark
spark = SparkSession.builder \
    .appName("MultiplyAndSaveAsText") \
    .master("local[*]") \
    .getOrCreate()

# Sample data
data = [(1,), (2,), (3,), (4,), (5,)]
df = spark.createDataFrame(data, ["number"])

# Multiply by 2
df2 = df.withColumn("number", col("number") * 2)

# Cast to string and save as text
output_path = "E:/Seekho Big Data/PySpark/TestOutputDumps/final_df_output"
if os.path.exists(output_path):
    shutil.rmtree(output_path)

df2.selectExpr("CAST(number AS STRING) AS number").write.text(output_path)

spark.stop()
