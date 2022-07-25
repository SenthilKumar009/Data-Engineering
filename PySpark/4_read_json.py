# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
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

df = spark.read.csv("/Users/Dell/PycharmProjects/PySprak/InputData/zipcodes.json")

df.printSchema()
df.show()

# Read multiline json file
multiline_df = spark.read.option("multiline","true") \
      .json("/Users/Dell/PycharmProjects/PySprak/InputData/multiline-zipcode.json")
multiline_df.show()

# Read multiple files
df2 = spark.read.json(
    ['/Users/Dell/PycharmProjects/PySprak/InputData/zipcode2.json', '/Users/Dell/PycharmProjects/PySprak/InputData/zipcode1.json'])
df2.show()
