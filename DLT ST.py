# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC create streaming table iot_device_stream_rtumati
# MAGIC (
# MAGIC   user_id long,
# MAGIC   calories_burnt FLOAT,
# MAGIC   num_steps LONG,
# MAGIC   miles_walked FLOAT,
# MAGIC   time_stamp timestamp,
# MAGIC   device_id long
# MAGIC ) 
# MAGIC comment "streaming table for IOT Device"
# MAGIC as SELECT
# MAGIC cast(user_id as long) as user_id,
# MAGIC cast(calories_burnt as FLOAT) as calories_burnt,
# MAGIC cast(num_steps as LONG) as num_steps,
# MAGIC cast(miles_walked as FLOAT) as miles_walked,
# MAGIC cast(timestamp as timestamp) as time_stamp,
# MAGIC cast(device_id as long) as device_id
# MAGIC from read_files("dbfs:/databricks-datasets/iot-stream/data-device/*.gz");
# MAGIC

# COMMAND ----------

# MAGIC %bash
# MAGIC dbfs ls dbfs:/FileStore/

# COMMAND ----------

print(_sqldf)
