# Perform a LEFT JOIN on two DataFrames (employees and salaries), group by department, and
# calculate average salary. Apply WHEN condition to determine salary categories and handle null
# values with COALESCE


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question2") \
    .getOrCreate()

data1= [(1, "John", "Sales"), (2, "Jane", "HR"), (3, "Mark", "Finance"), (4, "Emily", "HR")]

data1_schema="EmployeeID int, EmployeeName String, Designation String"

data2 = [
(1, 5000.00, "2024-01-10"),
(2, 6000.00, "2024-01-15"),
(4, 7000.00, "2024-01-20")
]

data2_schema="EmployeeID int, Salary double, Joining_Date string"

df1=spark.createDataFrame(data1, data1_schema)
df2=spark.createDataFrame(data2, data2_schema)

df3=df1.join(df2, on="EmployeeID", how="Left")

df3=df3.withColumn("Salary", coalesce(col("Salary"), lit(0.00))) \
    .withColumn("Joining_Date", coalesce(col("Joining_Date"), lit("2024-01-01")))
df3=df3.withColumn("Joining_Date", to_date(col("Joining_Date"), "yyyy-MM-dd"))

df4=df3.groupBy(col("Designation")).agg(avg(col("Salary")).alias("AvgSalary"))
df5=df3.withColumn("SalaryBand", when(col("Salary")<1000, "Low").when((col("Salary")>=1000)&(col("Salary")<5000), "Medium").otherwise("High"))



# df1.show()
# df2.show()
df3.show()
df4.show()
df5.show()




