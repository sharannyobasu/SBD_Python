# Perform a FULL OUTER JOIN between two DataFrames (departments and budget), fill missing
# values in budget, group by department, and calculate the total budget using SUM. Use DATE_ADD
# to forecast future budgets.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("Question 3") \
    .getOrCreate()

departments=[(101, "HR"),
(102, "IT"),
(103, "Finance"),
(104, "Marketing")]

schema1="EmployeeID int, Designation string"
df1=spark.createDataFrame(departments, schema1)

budget=[(101, 50000, "2024-05-01"),
(103, 75000, "2024-06-01"),
(None, 60000, "2024-06-15")]

schema2="EmployeeID int, Salary int, DOJ string"
df2=spark.createDataFrame(budget, schema2)

df3=df1.join(df2, on="EmployeeID", how="Outer")

avgSalaryDF=df3.dropna(subset=["Salary"])
#avgSalaryDF.show()

avgSalary=avgSalaryDF.agg(avg("Salary")).collect()[0][0]


df3=df3.withColumn("EmployeeID", coalesce(col("EmployeeID"), lit(9999))) \
    .withColumn("Designation", coalesce(col("Designation"), lit("Unknown"))) \
    .withColumn("Salary", coalesce(col("Salary"), lit(avgSalary)))
df3.show()