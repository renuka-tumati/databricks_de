# Databricks notebook source
# MAGIC %sql
# MAGIC /*CREATE TABLE orders (
# MAGIC     orderId int,
# MAGIC     orderTime timestamp,
# MAGIC     orderdate date GENERATED ALWAYS AS (CAST(orderTime as DATE)),
# MAGIC     units int)*/
# MAGIC
# MAGIC   CREATE TABLE orders (
# MAGIC     orderId int,
# MAGIC     orderTime timestamp,
# MAGIC     orderdate date AS (CAST(orderTime as DATE)),
# MAGIC     units int)

# COMMAND ----------

# MAGIC %md
# MAGIC Overall explanation
# MAGIC The answer is, GENERATED ALWAYS AS (CAST(orderTime as DATE))
# MAGIC
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch#--use-generated-columns
# MAGIC
# MAGIC Delta Lake supports generated columns which are a special type of columns whose values are automatically generated based on a user-specified function over other columns in the Delta table. When you write to a table with generated columns and you do not explicitly provide values for them, Delta Lake automatically computes the values.
# MAGIC
# MAGIC Note: Databricks also supports partitioning using generated column

# COMMAND ----------

# MAGIC %md
# MAGIC The data engineering team noticed that one of the job fails randomly as a result of using spot instances, what feature in Jobs/Tasks can be used to address this issue so the job is more stable when using spot instances?
# MAGIC
# MAGIC Here's how the team can use SIIN to make their jobs more stable when using spot instances:
# MAGIC
# MAGIC RETRY is the one of the option
# MAGIC
# MAGIC Detect Interruption Notices: 
# MAGIC
# MAGIC Configure their job or task to listen for SIIN events. This can be done by polling a metadata endpoint provided by the cloud provider or by subscribing to event notifications.
# MAGIC
# MAGIC Graceful Shutdown: Upon receiving an interruption notice, gracefully shut down the job or task to ensure that any in-flight data is processed or persisted correctly before the instance is terminated. This may involve flushing buffers, committing transactions, or checkpointing data.
# MAGIC
# MAGIC Handle Restart: If the job or task needs to be restarted after the interruption, ensure that it can resume from the last known checkpoint or state to avoid reprocessing already processed data. This can help minimize the impact of interruptions on job completion time and resource utilization.
# MAGIC
# MAGIC Fallback to On-Demand Instances: If job stability is critical and interruptions are causing significant disruptions, consider using a mix of spot and On-Demand instances or switching entirely to On-Demand instances for critical workloads. While this may increase costs, it can provide more predictability and stability for important jobs.
# MAGIC
# MAGIC Overall explanation
# MAGIC The answer is,  Add a retry policy to the task
# MAGIC
# MAGIC
# MAGIC
# MAGIC Tasks in Jobs support Retry Policy, which can be used to retry a failed tasks,  especially when using spot instance it is common to have failed executors or driver.

# COMMAND ----------

# MAGIC %md
# MAGIC What is the main difference between AUTO LOADER  and COPY INTO?
# MAGIC
# MAGIC Overall explanation
# MAGIC Auto loader supports both directory listing and file notification but COPY INTO only supports directory listing.
# MAGIC
# MAGIC
# MAGIC
# MAGIC Auto loader file notification will automatically set up a notification service and queue service that subscribe to file events from the input directory in cloud object storage like Azure blob storage or S3. File notification mode is more performant and scalable for large input directories or a high volume of files.

# COMMAND ----------

# MAGIC %md
# MAGIC
