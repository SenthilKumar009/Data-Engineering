import os
import sys
from sys import stdin
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, LongType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["language", "users_count"]
data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]

# Create a RDD using list data
rdd = spark.sparkContext.parallelize(data)

# Create DF using toDF method
df = rdd.toDF()

# Display schema of the DF
df.printSchema()
df.show()

df2 = rdd.toDF(columns)
df2.show()

# Create DF using createDataFrame method
dataDF = spark.createDataFrame(data=data, schema = columns)
dataDF.printSchema()
dataDF.show()

# Create DataFrame using StructType

dataSchema = StructType([
    StructField('lang_name', StringType(), True),
    StructField('total_count', LongType(), True)
])

deptDF1 = spark.createDataFrame(data=data, schema = dataSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)
