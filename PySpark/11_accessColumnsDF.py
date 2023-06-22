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

#colObj = lit("sparkbyexamples.com")

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Access column values
data = [("James", 23), ("Ann", 40)]
df = spark.createDataFrame(data).toDF("name.fname", "gender")
df.printSchema()

# Using DataFrame object (df)
df.select(df.gender).show()
df.select(df["gender"]).show()

# Accessing column name with dot (with backticks)
df.select(df["`name.fname`"]).show()

# Using SQL col() function
df.select(col("gender")).show()

# Accessing column name with dot (with backticks)
df.select(col("`name.fname`")).show()

# Create DataFrame with struct using Row class
data = [Row(name="James", prop=Row(hair="black", eye="blue")),
        Row(name="Ann", prop=Row(hair="grey", eye="black"))]
df = spark.createDataFrame(data)
df.printSchema()

# Access struct column
df.select(df.prop.hair).show()
df.select(df["prop.hair"]).show()
df.select(col("prop.hair")).show()

# Access all columns from struct
df.select(col("prop.*")).show()

# Python Column Operations

data=[(100,2,1),(200,3,4),(300,4,4)]
df=spark.createDataFrame(data).toDF("col1","col2","col3")

# Arithmetic operations
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show()
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
df.select(df.col1 % df.col2).show()

df.select(df.col2 > df.col3).show()
df.select(df.col2 < df.col3).show()
df.select(df.col2 == df.col3).show()

data=[("James", "Bond", "100", None),
      ("Ann", "Varsa", "200", 'F'),
      ("Tom Cruise", "XXX", "400", ''),
      ("Tom Brand", None, "400", 'M')]
columns = ["fname", "lname", "id", "gender"]
df = spark.createDataFrame(data, columns)
df.show()

# Alais
df.select(df.fname.alias("first_name"), df.lname.alias("last_name")).show()

# Another example
df.select(expr(" fname ||','|| lname").alias("fullName")).show()

# asc, desc to sort ascending and descending order repsectively.
df.sort(df.fname.asc()).show()
df.sort(df.fname.desc()).show()

# cast
df.select(df.fname,df.id.cast("int")).printSchema()

# between
df.filter(df.id.between(100, 300)).show()

# contains
df.filter(df.fname.contains("Cruise")).show()

# startswith, endswith()
df.filter(df.fname.startswith("T")).show()
df.filter(df.fname.endswith("Cruise")).show()

# isNull & isNotNull
df.filter(df.lname.isNull()).show()
df.filter(df.lname.isNotNull()).show()

# like, rlike
df.select(df.fname, df.lname, df.id).filter(df.fname.like("%om"))

# substr()
df.select(df.fname.substr(1,2).alias("substr")).show()


# when & otherwise

df.select(df.fname, df.lname, when(df.gender == "M", "Male")\
          .when(df.gender == "F", "Female")\
          .when(df.gender == None, "")\
          .otherwise(df.gender)\
          .alias("new_gender"))\
          .show()

# isin
li = ["100", "200"]
df.select(df.fname, df.lname, df.id).filter(df.id.isin(li)).show()
