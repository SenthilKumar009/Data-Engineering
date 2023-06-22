# Import SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql import Row
from pyspark.sql.functions import expr
from pyspark.sql.functions import when

import os
import sys
from sys import stdin

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")]

columns = ["firstname", "lastname", "country", "state"]
df = spark.createDataFrame(data=data, schema=columns)
df.show(truncate=False)

# 1. Select Single & Multiple Columns From PySpark
df.select("firstname", "lastname").show()
df.select(df.firstname, df.lastname).show()
df.select(df["firstname"], df["lastname"]).show()

# By using col() function
df.select(col("firstname"), col("lastname")).show()

# Select columns by regular expression
df.select(df.colRegex("`^.*name*`")).show()

# 2. Select All Columns from List
df.select(*columns).show()

# Select All columns
df.select([col for col in df.columns]).show()
df.select("*").show()

# 3. Selects first 3 columns and top 3 rows
df.select(df.columns[:3]).show(3)

# Selects columns 2 to 4  and top 3 rows
df.select(df.columns[2:4]).show(3)

# 4. Select Nested Struct Columns from PySpark
data = [(("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")]

schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])), StructField('state', StringType(), True),
              StructField('gender', StringType(), True)])
df2 = spark.createDataFrame(data = data, schema = schema)
df2.printSchema()
df2.show(truncate=False) # shows all columns

df2.select("name").show(truncate=False)

df2.select("name.firstname","name.lastname").show(truncate=False)

df2.select("name.*").show(truncate=False)


