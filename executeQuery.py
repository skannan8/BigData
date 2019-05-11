# Import data types
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import time
import numpy as np
import pandas as pd

start_time = time.time()
##### init session  #####
spark = SparkSession \
    .builder \
    .master("spark://c220g2-031131vm-1.wisc.cloudlab.us:7077")\
    .config("spark.eventLog.enabled", "true")\
    .config("spark.eventLog.dir", "/users/amogh3/logs")\
    .config("spark.driver.memory", "8g")\
    .config("spark.executor.memory", "8g")\
    .config("spark.executor.cores", "5")\
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext
spark.catalog.clearCache()
##### load input #####
#file = "US_census_data.csv"
#file = "tree.csv"
#file = "tree_sample_0.95.csv"
#file = "tree_uni_0.6.csv"
file = "Stratified/13/13/tree_sample_13_200K.csv"
# Load a text file and convert each line to a Row.
lines = sc.textFile(file)
parts = lines.map(lambda l: l.split(","))

# Each line is converted to a tuple.
input_RDD = parts.map(lambda p :tuple(p)[0:45])
##input_RDD = parts.map(lambda p :list(p)[0:45])
##print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$",input_RDD.collect(),"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$444")
#input_data = np.array(parts.collect())
#
##### Run the given query on a given RDD #####
def run_query(input_RDD, query):
	#start_time = time.time()
	# The schema is encoded in a string.
	schemaString = "tree_id block_id created_at tree_dbh stump_diam curb_loc status health spc_latin spc_common steward_guards sidewalk user_type problems root_stone root_grate root_other trunk_wire trnk_light trnk_other brch_light brch_shoe brch_other address postcode zip_city community board borocode borough cncldist st_assem st_senate nta nta_name boro_ct state latitude longitude x_sp y_sp council_district census_tract bin bbl"
	#schemaString = header.replace(',', ' ')
	
	fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
	schema = StructType(fields)
	#print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$",schema,"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
	# Apply the schema to the RDD.
	schemaPeople = spark.createDataFrame(input_RDD, schema)
	
	# Creates a temporary view using the DataFrame
	schemaPeople.createOrReplaceTempView("my_table")
	#start_time = time.time()
	# SQL can be run over DataFrames that have been registered as a table.
	results = spark.sql(query)
	end_time = time.time()
	total_time = (end_time - start_time) * 1000
	results.show()
	
	# unpersist
	input_RDD.unpersist()
	schemaPeople.unpersist()
	results.unpersist()
	print("Time taken for the query: ", total_time, " ms")
	return

##### Main method #####
def main():

	#start_time = time.time()
	#file = "tree.csv"
	#file = "tree_sample_0.6.csv"
	#file = "tree_strat_sample_0.1.csv"
	#lines = sc.textFile(file)
	#parts = lines.map(lambda l: l.split(","))

	# Each line is converted to a tuple.
	#input_RDD = parts.map(lambda p :tuple(p)[0:45])

	input_queries = get_input_queries()

	run_query(input_RDD, input_queries[8])
	#print("Actual time is: ", actual_time, "seconds")
	#end_time = time.time()
	#total_time = end_time - start_time
	#print("Time taken for the query: ", total_time)

def get_input_queries():
	input_queries = list()
	q1 = "SELECT COUNT(*) FROM my_table" # (only uniform)
	q2 = "SELECT COUNT(stump_diam) FROM my_table GROUP BY status" #(uniform + strat(6))
	q3 = "SELECT AVG(tree_dbh) FROM my_table" # (only uniform)
	q4 = "SELECT COUNT(tree_dbh) FROM my_table WHERE tree_dbh >= (SELECT AVG(tree_dbh) FROM my_table)" # (uniform + strat(9))
	q5 = "SELECT SUM(tree_dbh) FROM my_table GROUP BY health" # (uni + strat(7))
	q6 = "SELECT COUNT(tree_dbh) FROM  my_table WHERE health = 'Fair'AND spc_common = 'honeylocust'" # (uni + strat(9))
	
	q7 = "SELECT MAX(X.avg) FROM (SELECT AVG(tree_dbh) as avg FROM my_table GROUP BY status)X";
#	q7 = "SELECT AVG(tree_dbh) FROM my_table WHERE health = 'Fair'" # (uni + strat(7))
	q8 = "SELECT problems, COUNT(user_type) FROM my_table GROUP BY problems" # (uni + strat(13))
	q9 = "SELECT COUNT(tree_dbh) FROM my_table WHERE tree_dbh >= (SELECT AVG(tree_dbh) FROM my_table) GROUP BY problems" # (uni + strat(13))
	q10 = "SELECT SUM(tree_dbh) FROM my_table GROUP BY health" # (uni)
	input_queries.append(q1)
	input_queries.append(q2)
	input_queries.append(q3)
	input_queries.append(q4)
	input_queries.append(q5)
	input_queries.append(q6)
	input_queries.append(q7)
	input_queries.append(q8)
	input_queries.append(q9)
	input_queries.append(q10)
	return input_queries

if __name__ == "__main__":
	main()
