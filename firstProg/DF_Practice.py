from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.Builder() \
    .appName("DF_Practice") \
    .master("local[*]") \
    .getOrCreate()

ddlschema="id int, name string, salary int, city string"

df=spark.read.format("csv") \
    .schema(ddlschema) \
    .option("header", True) \
    .option("mode", "DROPMALFORMED") \
    .option("path", "C:/Users/shara/Downloads/details.csv") \
    .load()

df.show(50)

# df2=df.withColumn("Aukaat", when(col("salary")>1000, "Ameer").otherwise("Gareeb"))
# df2.show(40)
#
# # to execute SQL statements using pySpark, first we create a view using createTempView
# df3=df.createTempView("IDENTIFIER")
# spark.sql("select * from identifier where salary > 1000").show()


data=[(1, 'Ajay', 28), (2, 'Vijay', 35), (3, 'Manoj', 22), (4, 'Aamir', 61), (5, 'Siddharth', 17)]
schema='id int, name string, age int'

df1=spark.createDataFrame(data, schema)

df1.withColumn("Adult/Child", when(col('Age')>=18, 'Adult').otherwise('Child')).show()
#df1.show()

df1.select(substring(col("Name"), 2, 3)).show()