# Problem:
# You have a DataFrame user_data with columns: user_id, email, signup_date, and last_login.
#
# -> Extract the domain from the email column using regex_extract.
# -> Convert the domain to uppercase.
# -> Filter out users whose email domain ends with ".org".
# -> Group by the extracted domain and calculate:
#     -> The number of users per domain.
#     -> The average days between signup_date and last_login.
# -> Drop duplicate user_ids before the group by.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
        .master("local[*]") \
        .appName("RegexAssignment") \
        .getOrCreate()

data=[("UOO1", "john.doe@gmail.com", "2024-01-01", "2024-03-01"),
      ("U002", "jane.smith@outlook.com", "2024-02-15", "2024-03-10"),
      ("U003", "alice.jones@yahoo.org", "2024-03-01", "2024-03-20")]

schema="user_id string, email string, signup_date string, last_login string"
df=spark.createDataFrame(data, schema)

df=df.withColumn("signup_date", to_date("signup_date", "yyyy-MM-dd"))
df=df.withColumn("last_login", to_date("last_login", "yyyy-MM-dd"))

# -> Extract the domain from the email column using regex_extract.
df=df.withColumn("email_domain", regexp_extract(col("email"), "[a-zA-Z0-9\.]+$", 0))

print("Domain extraction: ")
df.show()

# -> Convert the domain to uppercase.
df=df.withColumn("email_domain", upper(col("email_domain")))

print("Uppercase results: ")
df.show()

# -> Filter out users whose email domain ends with ".org".
df=df.filter(~col("email_domain").endswith(".ORG"))

print("Filtered results: ")
df.show()


# -> Group by the extracted domain and calculate:
#     -> The number of users per domain.
#     -> The average days between signup_date and last_login.
df_groupby=df.withColumn("datediff", datediff(col("last_login"), col("signup_date"))) \
    .groupby(col("email_domain")) \
    .agg(count(col("user_id")).alias("number_of_users"), avg(col("datediff")).alias("average_difference"))

print("Group By results: ")
df_groupby.show()

# -> Drop duplicate user_ids before the group by.
df=df.drop_duplicates()

print("Dropped Duplicates Results: ")
df.show()
