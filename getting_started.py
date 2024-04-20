# Databricks notebook source
# MAGIC %md
# MAGIC ### This is test notebook

# COMMAND ----------

print("hello")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS rtumati.diamonds;
# MAGIC CREATE TABLE rtumati.diamonds USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rtumati.diamonds;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS rtumati

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS rtumati.customer_implicit;
# MAGIC CREATE TABLE rtumati.customer_implicit USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true");
