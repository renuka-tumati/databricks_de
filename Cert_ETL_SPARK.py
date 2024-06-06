# Databricks notebook source
from pyspark.sql.functions import count_if, col

# Create a sample DataFrame
data = [("a", 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)]
df_count = spark.createDataFrame(data, ["c1", "c2"])

# Count the number of even values in column 'c2'
result = df_count = spark.createDataFrame(data, ["c1", "c2"]).select(count_if((col("c2") % 2) == 1)).show()


# COMMAND ----------

import pandas as pd

data = {
    "A": [1, 1, 1],
    "B": [2, 3, 2],
    "C": ["00X", "010", "002"]
}

df = pd.DataFrame(data)
print(df)
# Create a boolean mask for rows where A and B are not duplicated
m1 = ~df[['A', 'B']].duplicated()

# Create a boolean mask for rows where C contains 'X' and C is not duplicated
m2 = df['C'].str.contains('X') & ~df['C'].duplicated()

# Filter the rows based on the masks
df_dedup = df[m1 | m2]

print(df_dedup)


# COMMAND ----------

import pandas as pd

data = {
    "Name": ["Alice", "Bob", "Alice", "Charlie", "Bob"],
    "Age": [25, 30, 25, 22, 30]
}

df = pd.DataFrame(data)
print(df)

# Deduplicate based on the "Name" column
df_dedup = df.drop_duplicates(subset=["Name"])

print(df_dedup)

# COMMAND ----------



# Read data from CSV file (assuming it has columns 'name' and 'email')
df = spark.read.csv("/path/to/customers.csv", header=True, inferSchema=True)

# Count occurrences where email is null
count_null_email = df.where(df['email'].isNull()).count()

# Print the result
print("Number of customers with missing email addresses:", count_null_email)


# COMMAND ----------

display(sqlContext.sql("DESCRIBE EXTENDED rtumati.check_points"))

# COMMAND ----------

display(sqlContext.sql("DESCRIBE formatted rtumati.check_points"))

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming table cannot do describe extended
# MAGIC

# COMMAND ----------

display(spark.sql("describe detail main.rtumati.users"))

# COMMAND ----------

from datetime import datetime

# Set a timestamp value
timestamp_value = datetime(2024, 5, 8, 15, 30, 0)  # Example: May 8, 2024, 3:30 PM

# Convert timestamp value to string format
timestamp_str = timestamp_value.strftime("%Y-%m-%d %H:%M:%S")

# Execute SQL statement to select and verify the timestamp value
query = f"""
SELECT '{timestamp_str}' AS timestamp_value
"""

result_df = spark.sql(query)
result_df.show()

# Assuming you have a DataFrame named 'df' and a UC volume path '/mnt/uc_volume/'

display(spark.sql("SHOW VOLUMES"))

# Specify the location of the UC volume
uc_volume_path = '/Volumes/main/rtumati/rtumati_test_poc'

# Specify the name of the table
table_name = 'rt_timestamp'

# result_df.write.saveAsTable(table_name)

# Write the DataFrame to the UC volume location as a table
#result_df.write.format('delta').save(uc_volume_path + table_name)


# Execute SQL statement to select calendar data from timestamp_column in my_table

calendar_query = f"""
SELECT 
    year(timestamp_value) AS year,
    month(timestamp_value) AS month,
    day(timestamp_value) AS day,
    hour(timestamp_value) AS hour,
    minute(timestamp_value) AS minute,
    second(timestamp_value) AS second
FROM 
    rt_timestamp;
"""

display(spark.sql(calendar_query))

calendar_query2 = """SELECT
    extract(DAY FROM timestamp_value) AS event_day,
    extract(MONTH FROM timestamp_value) AS event_month,
    extract(YEAR FROM timestamp_value) AS event_year
FROM rt_timestamp;"""

display(spark.sql(calendar_query2))



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

# Create a Spark session
spark = SparkSession.builder.appName("StringPatternExtraction").getOrCreate()

# Sample data
data = [("John Doe",), ("Jane Smith",), ("Michael Johnson",)]
df = spark.createDataFrame(data, ["name"])

# Extract the first word from the "name" column
df_with_extracted_pattern = df.withColumn("first_name", regexp_extract("name", r"^\w+", 0))

df_with_extracted_pattern.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Dot Syntax Example") \
    .getOrCreate()

# Sample DataFrame with nested data fields
data = [("John", {"age": 30, "address": {"city": "New York", "zipcode": "10001"}}),
        ("Alice", {"age": 25, "address": {"city": "San Francisco", "zipcode": "94101"}})]

columns = ["name", "info"]
df = spark.createDataFrame(data, columns)

# Use dot syntax to extract nested data fields
df = df.withColumn("age", col("info.age")) \
       .withColumn("city", col("info.address.city")) \
       .withColumn("zipcode", col("info.address.zipcode")) \
       .drop("info")  # Drop the nested column

# Show the DataFrame with extracted nested data fields
df.show()

# Stop the SparkSession
spark.stop()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("NestedDataExtraction").getOrCreate()

# Sample data with nested structure
data = [
    ("John Doe", {"Car": {"Make": "Ford", "Year": 2008}}),
    ("Jane Smith", {"Car": {"Make": "Toyota", "Year": 2015}})
]

columns = ["Name", "Details"]
df = spark.createDataFrame(data, columns)
df.show()

# Extract nested fields using dot syntax
df_with_extracted_fields = df.withColumn("Car_Make", col("Details.Car.Make"))
df_with_extracted_fields.show()

# Access specific nested field value
specific_value = df_with_extracted_fields.select("Car_Make").first()[0]
print(f"Specific Car Make: {specific_value}")


# COMMAND ----------

# MAGIC %md
# MAGIC JSON to struct

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("JSON Parsing Example") \
    .getOrCreate()

# Sample DataFrame with JSON strings
data = [("John", '{"age": 30, "city": "New York", "zipcode": "10001"}'),
        ("Alice", '{"age": 25, "city": "San Francisco", "zipcode": "94101"}')]

schema = StructType([
    StructField("age", IntegerType()),
    StructField("city", StringType()),
    StructField("zipcode", StringType())
])

columns = ["name", "json_string"]
df = spark.createDataFrame(data, columns)

# Parse JSON strings into structs
df = df.withColumn("json_struct", from_json(col("json_string"), schema))

# Show the DataFrame with parsed JSON structs
df.show(truncate=False)




# COMMAND ----------

# MAGIC %md
# MAGIC wide format to long format

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Pivot Example") \
    .getOrCreate()

# Sample DataFrame in wide format
data = [("John", 30, 150, 70),
        ("Alice", 25, 160, 65),
        ("Bob", 35, 170, 75)]

columns = ["name", "age", "height_cm", "weight_kg"]
df = spark.createDataFrame(data, columns)
display(df)

# Convert data from wide to long format using pivot
long_df = df.select("name", 
                    col("age").alias("attribute"), 
                    col("height_cm").alias("value")).union(
           df.select("name", 
                    col("weight_kg").alias("attribute"), 
                    col("weight_kg").alias("value")))

# Show the DataFrame in long format
long_df.show()

# Stop the SparkSession
spark.stop()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Simple SQL UDF Example") \
    .getOrCreate()

# Sample DataFrame
data = [("John", 30),
        ("Alice", 25),
        ("Bob", 35)]

columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Define a simple UDF to concatenate "Hello, " with the name
@udf(StringType())
def greet(name):
    return "Hello, " + name

# Apply the UDF to the DataFrame
df = df.withColumn("greeting", greet(df["name"]))

# Show the DataFrame with the applied UDF
df.show()

# Stop the SparkSession
spark.stop()


# COMMAND ----------


# Define a Python function to calculate the square of a number
def calculate_square(input_number):
    return input_number * input_number

# Register the Python function as a SQL UDF
spark.udf.register("CalculateSquare", calculate_square)

# The UDF is now available to use in SQL queries
query = "select CalculateSquare(5)"

display(spark.sql(query))


display(spark.sql("describe function CalculateSquare"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a UDF to calculate the square of a number
# MAGIC CREATE OR REPLACE FUNCTION CalculateSquareSQL(inputNumber FLOAT)
# MAGIC RETURNS FLOAT
# MAGIC LANGUAGE SQL
# MAGIC AS  $$
# MAGIC     SELECT inputNumber * inputNumber AS result
# MAGIC $$
# MAGIC
