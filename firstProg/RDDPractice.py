#Transformations discussed here: map, filter, distinct, map with a function, reduceByKey
#Actions discussed here: take, first, collect, countByKey


from pyspark import SparkContext

sc= SparkContext("local[2]","RDDPractice")

customer_data = [
    "customer_id,name,city,state,country,registration_date,is_active",
    "0,Customer_0,Bangalore,Karnataka,India,2023-11-11,True",
    "1,Customer_1,Hyderabad,Delhi,India,2023-08-26,True",
    "2,Customer_2,Ahmedabad,West Bengal,India,2023-06-23,True",
    "3,Customer_3,Bangalore,Tamil Nadu,India,2023-03-24,False",
    "4,Customer_4,Bangalore,Gujarat,India,2023-06-06,False",
    "5,Customer_5,Delhi,Maharashtra,India,2023-04-19,False",
]

rdd1=sc.parallelize(customer_data)
#print(rdd1.getNumPartitions())  #see how many partitions are being created for this RDD

header=rdd1.first() #extract first row from the rdd
#print(header)

rdd2=rdd1.filter(lambda x:x!=header)
#print(rdd2.collect())   #printing the rdd. First we call collect() which is an action and then print it

def parse_row(row):
    fields=row.split(",")
    return (fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]=='True')

parsed_rdd=rdd2.map(parse_row)

#only select selective columns from entire rdd
name_city_rdd=parsed_rdd.map(lambda x: (x[1], x[2]))


#filter out active customers (is_active column contains True value)
active_customers=parsed_rdd.filter(lambda x:x[6]==True)
#print(active_customers.collect())

#find distinct cities. Remember, distinct is a transformation.
cities=parsed_rdd.map(lambda x:x[2]).distinct()
#print(cities.collect())

customers_per_city=parsed_rdd.map(lambda x:(x[2], 1)).reduceByKey(lambda x,y:(x+y))
#print(customers_per_city.collect())

final_rdd=parsed_rdd.map(lambda x:x[2]).countByValue()  #countByValue() is an action hence no collect() on this.
#print(final_rdd)


#find list of cities with active customers
cities_with_active_customers=active_customers.map(lambda x:x[3]).distinct()
#print(cities_with_active_customers.collect())


#find count of active customers by state
active_customers_by_state=parsed_rdd.filter(lambda x:x[6]==True).map(lambda x:(x[3], 1))
print(active_customers_by_state.collect())