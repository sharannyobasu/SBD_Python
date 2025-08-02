# Question 3: Employee Flags, Day of Week, Salary in Lakhs
# Task:
# Create employee_flag column:
# If designation starts with "Senior" or salary > 100000 -> "Leadership"
# If designation starts with "Intern" -> "Fresher"
# Else -> "Staff"
# Derive weekday_joined from join_date (e.g., Monday, Tuesday).
# Create salary_lakhs column = salary / 100000, formatted to 2 decimals.
# Extract name_initial = first letter of name.
# Create is_gmail_user = "Yes" if email contains "gmail", else "No".
# Rename name to emp_name."

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


# Create employee_flag column:
# If designation starts with "Senior" or salary > 100000 -> "Leadership"
# If designation starts with "Intern" -> "Fresher"
# Else -> "Staff"

df=df.withColumn("employee_flag", when((col("Designation").contains("Senior")) | (col("Salary")>100000), "Leadership").when(col("Designation").contains("Intern"), "Fresher").otherwise("Staff"))

# Derive weekday_joined from join_date (e.g., Monday, Tuesday).
df=df.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))
df=df.withColumn("dayOfWeek", date_format(col("join_date"), 'EEEE'))

# Create salary_lakhs column = salary / 100000, formatted to 2 decimals.
df=df.withColumn("SalaryInLakhs", round(col("Salary")/100000, 2))

# Extract name_initial = first letter of name.
df=df.withColumn("Initials", regexp_extract(col("Name"), "^[a-zA-Z]", 0))

# Create is_gmail_user = "Yes" if email contains "gmail", else "No".
df=df.withColumn("is_gmail_user", when(col("email").contains("gmail"), "Yes").otherwise("No"))

#final result
final_df=df.select(col("Name"), col("Initials"), col("Email"), col("Join_Date"), col("DayOfWeek"), col("Designation"), col("Employee_Flag"), col("SalaryInLakhs"), col("is_gmail_user"))
final_df.show()