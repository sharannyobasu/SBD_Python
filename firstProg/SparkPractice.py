from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import min, max

spark = SparkSession.builder.appName("MovieRatingsAnalysis_14June").getOrCreate()


kartik_14June = [
    ("City1", "2022-01-01", 10.0),
    ("City1", "2022-01-02", 8.5),
    ("City1", "2022-01-03", 12.3),
    ("City2", "2022-01-01", 15.2),
    ("City2", "2022-01-02", 14.1),
    ("City2", "2022-01-03", 16.8)]

schema_14June=["City", "Date", "Temperature"]

ratings_df = spark.createDataFrame(kartik_14June, schema=schema_14June)

movie_summary = ratings_df.groupBy("City").agg(
    avg("Temperature").alias("Average Temp"),
    min("Temperature").alias("Min Temp"),
max("Temperature").alias("Max Temp")
)

movie_summary.show()


spark.stop()