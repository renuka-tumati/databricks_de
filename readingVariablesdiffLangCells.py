# Databricks notebook source
import datetime
x = str(datetime.date.today())
dbutils.widgets.text("max_date", x)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL cell
# MAGIC SELECT getArgument("max_date") AS max_date;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # My Interoperability Example
# MAGIC - Here's a Python variable: ${my_variable} %python my_variable
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ${abc.test_var}

# COMMAND ----------

# Set the Python variable
max_date2 = str(datetime.date.today())
spark.conf.set("abc.max_dt2", max_date2)
dbutils.widgets.text("abc.max_date2", max_date2)


# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Scala cell
# MAGIC val date = dbutils.widgets.get("abc.max_date2")
# MAGIC println(s"Value of abc.max_date2: $date")
# MAGIC
# MAGIC // Scala cell
# MAGIC val date2 = spark.conf.get("abc.max_dt2")
# MAGIC println(s"Value of abc.max_dt2: $date2")
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL cell
# MAGIC SELECT "${abc.max_dt2}" AS max_date;
# MAGIC
