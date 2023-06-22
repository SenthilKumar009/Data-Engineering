# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import sys
from sys import stdin

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

# 1. Create DataFrame from RDD

columns = ["language", "users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()

columns = ["language", "users_count"]
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

dfFromRDD1.show()

# Using createDataFrame() from SparkSession

dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.show()

# Create DataFrame from List Collection

# List of Data
data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)]

schema = StructType([ \
  StructField("firstname", StringType(), True), \
  StructField("middlename", StringType(), True), \
  StructField("lastname", StringType(), True), \
  StructField("id", StringType(), True), \
  StructField("gender", StringType(), True), \
  StructField("salary", IntegerType(), True) \
  ])

df = spark.createDataFrame(data=data2, schema=schema)
df.printSchema()
df.show(truncate=False)

# 3. Create DataFrame from Data Sources

df2 = spark.read.csv("C:/Users/Dell/Downloads/Dataset/orders.csv", header=True)
df2.show()

#df2 = spark.read.text("/src/resources/file.txt")
#df2 = spark.read.json("/src/resources/file.json")
