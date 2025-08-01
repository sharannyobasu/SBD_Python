from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Assuming 'spark' is an initialized SparkSession
spark = SparkSession.builder.appName("YourApp").getOrCreate()

data = [
    ("Jane", 176, "Berlin", "Data Scientist", 39682),
    ("Charlie", 3, "Paris", "Project Manager", 100000),
    ("Robert", 64, "Mumbai", "Data Scientist", 62499),
    ("Michael", 48, "Berlin", "Software Engineer", 84493),
    ("Patricia", 108, "Mumbai", "Data Scientist", 73671),
    ("Patricia", 125, "Paris", "Marketing Manager", 93162),
    ("Jennifer", 66, None, "DevOps Engineer", 90630),
    ("Charlie", 3, "Paris", "Project Manager", 100000),
    ("Peter", 192, "Dubai", "HR Specialist", 32664),
    ("Politano", 6, "London", "Marketing Manager", 80000),
    ("Jennifer", 165, "Sydney", "Data Analyst", 91869),
    ("Robert", 114, "Sydney", "Product Manager", 97361),
    ("Susan", 73, "London", "HR Specialist", 75057),
    ("Steven", 160, "Tokyo", "Data Analyst", 44405),
    ("Robert", 140, "Dubai", "Software Engineer", 36871),
    ("Steve", 2, "London", "Data Analyst", 75000),
    ("Robert", 91, "Mumbai", "Product Manager", 119103),
    ("Peter", 61, "Paris", "UX Designer", 100644),
    ("Steve", 2, "London", "Data Analyst", 75000),
    ("Chris", 77, "New York", "DevOps Engineer", 93626),
    ("Susan", 172, "Toronto", "UX Designer", 95745),
    ("Susan", 183, "Sydney", "Product Manager", 56623),
    ("Susan", 137, "Singapore", "UX Designer", 63707),
    (None, 7, "Tokyo", "Intern", 30000),
    ("Grace", 8, None, "DevOps Engineer", 88000),
    (None, 148, "Dubai", "Product Manager", 104160),
    ("Patricia", 84, "Dubai", "Data Analyst", 115365),
    ("Steven", 45, None, "Data Scientist", 62033),
    ("Peter", 103, "Singapore", "HR Specialist", 40401),
    ("Patricia", 81, "New York", "Data Scientist", 99621),
    ("Michael", 197, "Paris", "HR Specialist", 97565),
    ("Peter", 185, "New York", "Data Analyst", 114901),
    ("Mary", 177, "Singapore", "Intern", 90179),
    ("Politano", 6, "London", "Marketing Manager", 80000),
    ("Steven", 144, "London", "UX Designer", 49238),
    ("Robert", 107, "Sydney", "Project Manager", 103443),
    ("Mary", 126, "Sydney", "DevOps Engineer", 96028),
    ("David", 4, "Berlin", "HR Specialist", 60000),
    ("Jane", 120, "Mumbai", "DevOps Engineer", 47283),
    ("John", 98, "Paris", "Software Engineer", 69130),
    ("Jane", 152, "Dubai", "Data Analyst", 101603),
    ("Steven", 147, "Sydney", "Software Engineer", 80587),
    ("Charlie", 3, "Paris", "Project Manager", 100000),
    ("John", 121, "Singapore", "Data Scientist", 108062),
    ("Steve", 2, "London", "Data Analyst", 75000),
    ("Susan", 196, None, "Software Engineer", 55382),
    ("Dave", 95, "Toronto", "DevOps Engineer", 33853),
    ("Robert", 129, "Tokyo", "UX Designer", 80491),
    ("David", 4, "Berlin", "HR Specialist", 60000),
    ("Politano", 6, "London", "Marketing Manager", 80000),
    ("Dave", 188, None, "HR Specialist", 112552),
    ("Grace", 8, None, "DevOps Engineer", 88000),
    (None, 199, "London", "DevOps Engineer", 58724),
    ("Grace", 8, None, "DevOps Engineer", 88000),
    ("Michael", 87, "Toronto", "HR Specialist", 42421),
    ("Dave", 90, "Berlin", "HR Specialist", 82367),
    ("Michael", 169, "Tokyo", "Product Manager", 117956),
    ("Mary", 97, "London", "HR Specialist", 47810),
    ("John", 194, "Dubai", "UX Designer", 90428),
    ("Peter", 171, "Tokyo", "Project Manager", 47603),
    (None, 7, "Tokyo", "Intern", 30000),
    ("John", 67, None, "Project Manager", 74224),
    (None, 110, "Singapore", "Project Manager", 51341),
    ("Charlie", 3, "Paris", "Project Manager", 100000),
    ("Chris", 105, "Tokyo", "Intern", 81347),
    ("Patricia", 83, "London", "Project Manager", 57724),
    ("Christina", 178, "Berlin", "DevOps Engineer", 104741),
    (None, 7, "Tokyo", "Intern", 30000),
    ("John", 51, "Toronto", "HR Specialist", 99213),
    ("Grace", 8, None, "DevOps Engineer", 88000),
    ("Steven", 127, "London", "Intern", 65817),
    ("Peter", 111, "London", "Product Manager", 41737),
    ("Politano", 6, "London", "Marketing Manager", 80000),
    ("Politano", 6, "London", "Marketing Manager", 80000),
    ("Patricia", 71, "Dubai", "Data Analyst", 73985),
    ("Steven", 138, "New York", "Intern", 116443),
    ("Robert", 193, "Berlin", "Project Manager", 95302),
    ("Mary", 50, "Singapore", "Data Scientist", 107004),
    ("Christina", 93, "London", "Software Engineer", 109025),
    ("Peter", 88, None, "HR Specialist", 57521),
    ("Jennifer", 146, "London", "Intern", 100035),
    ("David", 4, "Berlin", "HR Specialist", 60000),
    ("John", 78, None, "Product Manager", 47461),
    ("Michael", 143, "London", "DevOps Engineer", 86394),
    ("Jane", 119, "New York", "Data Scientist", 118536),
    ("Grace", 8, None, "DevOps Engineer", 88000),
    ("Mary", 63, "Berlin", "UX Designer", 44639),
    ("Jane", 142, "Sydney", "UX Designer", 85839),
    (None, 7, "Tokyo", "Intern", 30000),
    ("Jane", 136, "Mumbai", "Product Manager", 48071),
    ("Christina", 155, "Paris", "Intern", 50303),
    ("Mary", 85, "Singapore", "Software Engineer", 77180),
    ("Susan", 139, "London", "HR Specialist", 44512),
    ("Alice", 1, "New York", "Software Engineer", 90000),
    ("Jennifer", 60, "Berlin", "UX Designer", 86350),
    ("David", 4, "Berlin", "HR Specialist", 60000),
    ("Jennifer", 49, "Tokyo", "Marketing Manager", 73812),
    ("Jennifer", 186, "Berlin", "Data Scientist", 45215),
    (None, 7, "Tokyo", "Intern", 30000),
    ("Robert", 157, "Singapore", "Project Manager", 112736),
    ("Cristiano", 5, "New York", "Data Scientist", 95000),
    ("Peter", 62, None, "UX Designer", 33960),
    ("Peter", 153, "Berlin", "Intern", 69317),
    (None, 69, "Mumbai", "Data Scientist", 113522),
    ("Christina", 162, "Mumbai", "Product Manager", 66206),
    ("Robert", 174, "Berlin", "HR Specialist", 51580),
    ("Susan", 102, "Dubai", "Marketing Manager", 79133),
    ("John", 191, "Dubai", "HR Specialist", 63710),
    ("Dave", 55, "New York", "Data Scientist", 109295),
    ("Cristiano", 5, "New York", "Data Scientist", 95000),
    ("Dave", 163, "Berlin", "Data Scientist", 46232),
    ("Michael", 135, None, "DevOps Engineer", 96570),
    ("Mary", 109, "London", "UX Designer", 42176),
    (None, 158, "Toronto", "Project Manager", 43506),
    ("Charlie", 3, "Paris", "Project Manager", 100000),
    ("Grace", 8, None, "DevOps Engineer", 88000),
    ("Steven", 72, "Toronto", "Data Scientist", 107442),
    ("David", 4, "Berlin", "HR Specialist", 60000),
    ("Cristiano", 5, "New York", "Data Scientist", 95000),
    ("Chris", 43, "Dubai", "HR Specialist", 55397),
    ("Steve", 2, "London", "Data Analyst", 75000),
    ("Chris", 159, "Sydney", "Data Scientist", 95279),
    ("Jennifer", 115, None, "Project Manager", 78066),
    ("Steve", 2, "London", "Data Analyst", 75000),
    ("Susan", 116, "Berlin", "Data Analyst", 35866),
    ("Christina", 100, "London", "HR Specialist", 52279),
    ("Cristiano", 5, "New York", "Data Scientist", 95000),
    ("Peter", 82, "Dubai", "DevOps Engineer", 64741),
    (None, 7, "Tokyo", "Intern", 30000),
    ("Alice", 1, "New York", "Software Engineer", 90000),
    ("Robert", 182, "Sydney", "Intern", 63525),
    ("Mary", 128, None, "Marketing Manager", 81451),
    ("Grace", 8, None, "DevOps Engineer", 88000),
    ("Steven", 54, "Mumbai", "DevOps Engineer", 86573),
    ("David", 4, "Berlin", "HR Specialist", 60000),
    ("Jane", 122, "Dubai", "Intern", 93942),
    ("Steve", 2, "London", "Data Analyst", 75000),
    (None, 7, "Tokyo", "Intern", 30000),
    ("Susan", 167, "Singapore", "Intern", 75587),
    ("Robert", 65, "Sydney", "Project Manager", 36960),
    ("Steven", 179, None, "Data Analyst", 104580),
    ("Michael", 173, "Toronto", "HR Specialist", 119335),
    ("Dave", 89, "Singapore", "HR Specialist", 57906),
    ("Susan", 123, None, "DevOps Engineer", 34575),
    ("Susan", 124, "Tokyo", "Data Scientist", 60911),
    ("John", 187, "Paris", "Product Manager", 51482),
    ("Mary", 184, "Mumbai", "Project Manager", 62384),
    ("Dave", 149, "New York", "Software Engineer", 91593),
    ("Mary", 92, "Mumbai", "DevOps Engineer", 40509),
    ("Jennifer", 195, "Berlin", "Data Analyst", 48981),
    ("Patricia", 156, None, "Data Analyst", 99316),
    ("John", 154, "Mumbai", "Marketing Manager", 104784),
    ("John", 134, "Berlin", "Data Analyst", 117220),
    ("Mary", 99, "Dubai", "Product Manager", 76284),
    (None, 74, "Berlin", "Intern", 81090),
    ("Susan", 190, None, "Data Scientist", 108533),
    ("David", 4, "Berlin", "HR Specialist", 60000),
    ("Cristiano", 5, "New York", "Data Scientist", 95000),
    ("Christina", 117, "Berlin", "Data Analyst", 86042),
    ("Steve", 2, "London", "Data Analyst", 75000),
    ("Dave", 104, "Dubai", "Data Analyst", 61508),
    ("Politano", 6, "London", "Marketing Manager", 80000),
    ("Chris", 181, "New York", "Project Manager", 76574),
    ("Mary", 168, None, "Marketing Manager", 73352),
    ("Alice", 1, "New York", "Software Engineer", 90000),
    ("Mary", 75, None, "DevOps Engineer", 33278),
    ("Alice", 1, "New York", "Software Engineer", 90000),
    ("Steven", 161, "Dubai", "UX Designer", 59788),
    (None, 112, "Berlin", "HR Specialist", 78562),
    ("Dave", 68, "Toronto", "UX Designer", 117101),
    ("Christina", 164, "Sydney", "Project Manager", 38877),
    ("Robert", 198, "London", "Project Manager", 44966),
    ("Jennifer", 166, "Dubai", "Software Engineer", 62023),
    ("Peter", 132, "New York", "Software Engineer", 86821),
    ("Robert", 70, "Paris", "UX Designer", 52827),
    ("Christina", 145, None, "Product Manager", 50639),
    ("Susan", 53, None, "Marketing Manager", 85623),
    ("Steve", 2, "London", "Data Analyst", 75000),
    ("Peter", 106, "Singapore", "Product Manager", 62047),
    ("Patricia", 86, "Singapore", "Product Manager", 98055),
    ("Steven", 175, "Berlin", "Software Engineer", 106918),
    ("Susan", 113, "Singapore", "Software Engineer", 68927),
    ("Dave", 170, "Paris", "Data Analyst", 87713),
    (None, 7, "Tokyo", "Intern", 30000),
    ("Cristiano", 5, "New York", "Data Scientist", 95000),
    (None, 46, "Singapore", "DevOps Engineer", 58929),
    ("Politano", 6, "London", "Marketing Manager", 80000),
    ("John", 200, "Dubai", "DevOps Engineer", 53872),
    (None, 59, "Toronto", "Software Engineer", 43471),
    ("Jennifer", 44, "Mumbai", "Marketing Manager", 39641),
    ("Chris", 42, "Sydney", "DevOps Engineer", 69352),
    ("Mary", 41, "Dubai", "UX Designer", 36388),
    ("Dave", 58, "Paris", "Data Analyst", 70388),
    ("Cristiano", 5, "New York", "Data Scientist", 95000),
    ("Alice", 1, "New York", "Software Engineer", 90000),
    ("Chris", 52, None, "HR Specialist", 99863),
    ("Grace", 8, None, "DevOps Engineer", 88000),
    ("Michael", 80, "Tokyo", "Software Engineer", 37731),
    ("Mary", 141, "Mumbai", "Product Manager", 95051),
    ("Michael", 133, "Sydney", "DevOps Engineer", 54904),
]

# Define the schema
schema = StructType([
    StructField('name', StringType(), True),
    StructField('id', IntegerType(), True),
    StructField('city', StringType(), True),
    StructField('Designation', StringType(), True),
    StructField('salary', IntegerType(), True)
])

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)


order_data_list = [
    ("Order1", "John", 100),
    ("Order2", "Alice", 200),
    ("Order3", "Bob", 150),
    ("Order4", "Alice", 300),
    ("Order5", "Bob", 250),
    ("Order6", "John", 400),
    ("Order7", "John", 100),
    ("Order8", "Alice", 200),
    ("Order9", "Bob", 150),
    ("Order10", "Alice", 300),
    ("Order11", "Bob", 250),
    ("Order12", "John", 400),
    ("Order13", "John", 100),
    ("Order14", "Alice", 200),
    ("Order15", "Bob", 150),
    ("Order16", "Alice", 300),
    ("Order17", "Bob", 250),
    ("Order18", "John", 400),
    ("Order19", "John", 100),
    ("Order20", "Alice", 200),
    ("Order21", "Bob", 150),
    ("Order22", "Alice", 300),
    ("Order23", "Bob", 250),
    ("Order24", "John", 400),
    ("Order25", "John", 100),
    ("Order26", "Alice", 200),
    ("Order27", "Bob", 150),
    ("Order28", "Alice", 300),
    ("Order29", "Bob", 250),
    ("Order30", "John", 400),
    ("Order31", "John", 100),
    ("Order32", "Alice", 200),
    ("Order33", "Bob", 150),
    ("Order34", "Alice", 300),
    ("Order35", "Bob", 250),
    ("Order36", "John", 400),
    ("Order37", "John", 100),
    ("Order38", "Alice", 200),
    ("Order39", "Bob", 150),
    ("Order40", "Alice", 300),
    ("Order41", "Bob", 250),
    ("Order42", "John", 400),
    ("Order43", "John", 100),
    ("Order44", "Alice", 200),
    ("Order45", "Bob", 150),
    ("Order46", "Alice", 300),
    ("Order47", "Bob", 250),
    ("Order48", "John", 400),
    ("Order49", "John", 100),
    ("Order50", "Alice", 200)
]

order_schema = StructType([
    StructField("OrderID", StringType(), True),
    StructField("Customer", StringType(), True),
    StructField("Amount", IntegerType(), True)
])

# Create the PySpark DataFrame
orderData = spark.createDataFrame(data=order_data_list, schema=order_schema)

# df.filter(col("Designation").endswith("ist") & col("Name").contains("C")).show()
x=df.filter(col("salary")==75000).count()
print(x)

#find the count of orders placed by each customer and total order amount for each customer.

orderData.groupBy("Customer").agg(count(col("OrderID")).alias("NumOfOrders"), sum((col("Amount")))).show()

orderData.createOrReplaceTempView("orders")
sumAmount=spark.sql("""SELECT * FROM ORDERS WHERE AMOUNT > 250""")
print("Showing SparkSQL Code: ")
sumAmount.show()
#
# df.show(50)