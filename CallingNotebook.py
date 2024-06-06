# Databricks notebook source
# MAGIC %run ./CalledNotebook

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val date = spark.conf.get("abc.max_dt2")
# MAGIC println(s"Value of abc.max_dt2: $date")

# COMMAND ----------

dbutils.notebook.run("./CalledNotebook2", timeout_seconds=600)


# COMMAND ----------

# MAGIC %run ./CalledNotebook2

# COMMAND ----------

# MAGIC %md
# MAGIC running note book using dbutils.notebook.run("./CalledNotebook2", timeout_seconds=600)
# MAGIC did not help to run below sql - esp to read the max_date , could not find the widget set max_date from calledNotebook2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL cell
# MAGIC --SELECT "${abc.max_dt2}" AS max_date2;
# MAGIC SELECT getArgument("max_date") AS max_date; 
# MAGIC

# COMMAND ----------


