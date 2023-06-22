# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import sys
from sys import stdin

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)

# 1. Covert using toDF()

df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

# Covert using toDF() by passing column names
deptColumns = ["dept_name", "dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

# 2. Using createDataFrame() function

deptDF = spark.createDataFrame(rdd, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# 3. Using createDataFrame() with StructType schema

deptSchema = StructType([
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema=deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)

