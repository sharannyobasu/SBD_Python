from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.Builder() \
    .master("local[*]") \
    .appName("DFPractice30June") \
    .getOrCreate()

person_email_data = [
    ("John Doe", "john.doe@example.com"),
    ("Jane Smith", "jane_smith123@sub.domain.org"),
    ("Alice Wonderland", "alice+tag@mail.co.uk"),
    ("Bob Developer", "bob-developer@dev.net"),
    ("Charlie Brown", "charlie.brown@mycompany.info"),
    ("User Internal", "user@singlehost"),
    ("Admin Local", "admin@192.168.1.100"),
    ("Very Long Name Email", "test.email@a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z"),
    ("Another Long User", "long.username.with.many.dots@domain.com"),
    ("Hyphenated Name", "another.email.with.hyphens@example-domain.com"),
    ("Short Name", "a@b.com"),
    ("Numeric User", "12345@numeric-domain.xyz"),
    ("Travel Enthusiast", "first.last@domain.travel"),
    ("Dashed Email", "email.with.dashes@dash-domain.ai")
    ]
schema="Name string, email string"
df=spark.createDataFrame(person_email_data, schema)

df.show()

df.withColumn("FirstName", substring_index(col("Name"), " ", 1)).withColumn("LastName", substring_index(col("Name"), " ", -1)).withColumn("Username", substring_index(col("email"), '@', 1)).show()
