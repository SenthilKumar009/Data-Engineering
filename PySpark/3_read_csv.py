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

df = spark.read.csv("/Users/Dell/PycharmProjects/PySprak/InputData/zipcodes.csv")

df.printSchema()

# User header option
df2 = spark.read.option("header", True) \
     .csv("/Users/Dell/PycharmProjects/PySprak/InputData/zipcodes.csv")
df2.printSchema()

df2.show()

df3 = spark.read.options(header = True, delimiter=",") \
     .csv("/Users/Dell/PycharmProjects/PySprak/InputData/zipcodes.csv")
df3.printSchema()

# Using StructType
schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("Zipcode",IntegerType(),True) \
      .add("ZipCodeType",StringType(),True) \
      .add("City",StringType(),True) \
      .add("State",StringType(),True) \
      .add("LocationType",StringType(),True) \
      .add("Lat",DoubleType(),True) \
      .add("Long",DoubleType(),True) \
      .add("Xaxis",IntegerType(),True) \
      .add("Yaxis",DoubleType(),True) \
      .add("Zaxis",DoubleType(),True) \
      .add("WorldRegion",StringType(),True) \
      .add("Country",StringType(),True) \
      .add("LocationText",StringType(),True) \
      .add("Location",StringType(),True) \
      .add("Decommisioned",BooleanType(),True) \
      .add("TaxReturnsFiled",StringType(),True) \
      .add("EstimatedPopulation",IntegerType(),True) \
      .add("TotalWages",IntegerType(),True) \
      .add("Notes",StringType(),True)

df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("/Users/Dell/PycharmProjects/PySprak/InputData/zipcodes.csv")
df_with_schema.printSchema()