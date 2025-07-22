# Problem:
# You have a DataFrame web_logs with columns: session_id, url, timestamp, and user_agent.
# Use split to extract the protocol, domain, and path from the url column.
# Convert the user_agent to lowercase.
# Filter out records where the path starts with "/admin".
# Group by domain and calculate:
# The average length of paths.
# The count of sessions per domain.
# Drop duplicates based on session_id.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.Builder() \
    .appName("Problem 4") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ('S001', 'https://example.com/home', '2024-07-01 10:00:00', 'Chrome/90.0'),
    ('S002', 'http://sample.org/contact', '2024-07-02 11:30:00', 'Firefox/85.0'),
    ('S003', 'https://example.com/admin', '2024-07-03 12:45:00', 'Safari/14.1')]

schema="Session_ID String, URL String, TimeStamp String, User_Agent String"

df=spark.createDataFrame(data, schema)

# Use split to extract the protocol, domain, and path from the url column.
df=df.withColumn("Protocol", split(col("URL"), '://').getItem(0))
df=df.withColumn("Domain", split(split(col("URL"), '://').getItem(1), "/").getItem(0))
df=df.withColumn("Path", regexp_extract(split(col("url"), "://").getItem(1), "\/[a-zA-Z0-9]+$", 0))

# Convert the user_agent to lowercase.
df=df.withColumn("user_agent", lower(col("User_Agent")))

# Filter out records where the path starts with "/admin"
df=df.filter(~(col("Path").like("/admin%")))

# Group by domain and calculate:
# The average length of paths.

df_pathLength=df.groupby("domain").agg(avg(length(col("path"))).alias("Average Length"))
df_pathLength.show()

# The count of sessions per domain.

df_sessionPerDomain=df.groupby("domain").agg(count(col("Session_ID")).alias("Sessions_Per_Domain"))
df_sessionPerDomain.show()

# Drop duplicates based on session_id.
df.dropDuplicates(subset=["session_id"])


df.show()