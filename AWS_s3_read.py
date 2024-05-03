# Databricks notebook source
# MAGIC %fs ls dbfs:/Repos

# COMMAND ----------

# Mount S3 bucket
dbutils.fs.mount(source = "s3a://your-bucket-name", mount_point = "/mnt/s3")

# Read file from mounted S3 bucket
df = spark.read.csv("/mnt/s3/path/to/your/file.csv")


# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------


df = spark.read.format("delta").load("https://rtumatidatabricks.s3.amazonaws.com/files/MT+cars.csv")
display(df)

# COMMAND ----------


df = spark.read.parquet("https://rtumatidatabricks.s3.amazonaws.com/files/House+price.parquet")
display(df)
