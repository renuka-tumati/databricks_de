# Databricks notebook source
df_dia = spark.table("rtumati.diamonds")
df_dia.show()
dbutils.data.summarize(df_dia)

# COMMAND ----------

df_describe = sqlContext.sql("DESCRIBE DETAIL rtumati.diamonds")
display(df_describe)

# COMMAND ----------

display(df_dia)


# COMMAND ----------

df_dia_fil=df_dia.select("carat", "cut").where("carat > 0.1").orderBy(1)
display(df_dia_fil)

# COMMAND ----------

df_parquet = spark.read.parquet("/Workspace/Users/renuka.tumati@slalom.com/databricks_de/House_price.parquet")
df_parquet.show()

# COMMAND ----------

spark_sql_df = spark.sql("""select * from rtumati.diamonds""")

# COMMAND ----------

# to display schema
spark_sql_df.schema
spark_sql_df.printSchema()

# COMMAND ----------

spark_sql_df.createOrReplaceTempView("diamonds_view")
spark_sql_df.write.mode("overwrite").saveAsTable("rtumati.diamonds_copy")

# COMMAND ----------

display(spark.sql("select * from rtumati.diamonds_copy"))
display(spark.sql("select * from diamonds_view"))

# COMMAND ----------

df = spark.createDataFrame(
    [
        (0, 1),
        (0,0),
        (1,0),
        (1,1)
    ],
    ('col_1', 'col_2')
)
df.show()

# COMMAND ----------

# MAGIC %fs ls dbfs:/Repos
# MAGIC

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/csv/car

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/wine-quality/README.md

# COMMAND ----------

# MAGIC %fs grep /databricks-datasets/".parquet$"

# COMMAND ----------

df_moore_cars= spark.read.csv("dbfs:/databricks-datasets/Rdatasets/data-001/csv/car/Moore.csv")
df_moore_cars.count()

# COMMAND ----------

# df_moore_cars_load= spark.read.load("dbfs:/databricks-datasets/Rdatasets/data-001/csv/car/Moore.csv", format="csv", header=False)
# df_moore_cars_load= spark.read.load("dbfs:/databricks-datasets/Rdatasets/data-001/csv/car/Moore.csv", format="csv", header=True, inferSchema=True)
# df_moore_cars_load= spark.read.load("dbfs:/databricks-datasets/Rdatasets/data-001/csv/car/Moore.csv", format="csv", header=False, inferSchema=True)
# Fastet is explicit
#explicitSchema=("A1 DOUBLE, A2 STRING, A3 STRING, A4 STRING, MYSCHEMA STRING")
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
explicitSchema = StructType(
  [
    StructField("A1",  DoubleType(), True),
    StructField("A2", StringType(), True),
    StructField("A3", StringType(), True),
    StructField("A4", StringType(), True),
    StructField("MYSCHEMA", StringType(), True)
  ]
   )
csv_path = "dbfs:/databricks-datasets/Rdatasets/data-001/csv/car/Moore.csv"
df_moore_cars_load= spark.read.load(csv_path, format="csv", header=False, schema=explicitSchema)
display(df_moore_cars_load)
print(df_moore_cars_load.A1)


# COMMAND ----------

spark.conf.set("context.path", csv_path)

# COMMAND ----------

# MAGIC %scala
# MAGIC val path_to_csv= spark.conf.get("context.path")
# MAGIC val ddl_stmt1 = spark.read.option("inferSchema",true).csv(path_to_csv).schema.toDDL
# MAGIC val ddl_stmt2 = spark.read.option("header", true).option("inferSchema",true).csv(path_to_csv).schema.toDDL
# MAGIC println(ddl_stmt1)
# MAGIC println(ddl_stmt2)
# MAGIC
# MAGIC val df = spark.read.format("csv").schema(ddl_stmt1).load(path_to_csv)
# MAGIC //val df = spark.read.format("csv").schema(ddl_stmt2).load(path_to_csv)
# MAGIC
# MAGIC df.show()

# COMMAND ----------

from pyspark.sql.functions import col
print(df_moore_cars_load.A1)

print(df_moore_cars_load["A1"])
print(col("A1"))

rev_df = df_moore_cars_load.filter(  (col("A4")=='low') & (col("A1") > 1) ) 
rev_df.show()

rev_df = df_moore_cars_load.select(col("A1").alias("NEW_A1"))
rev_df.show()

rev_df = df_moore_cars_load.selectExpr( "A1 > 1 as new_a1_bol")
rev_df.show()

rev_df.dropDuplicates().show()
rev_df.limit(4).show()
rev_df.show()

# COMMAND ----------



# COMMAND ----------

df_moore_cars_load.write.option("compression", "snappy").mode("overwrite").parquet("dbfs:/Workspace")

# COMMAND ----------

display(dbutils.fs.ls("path_to_parquet"))
