# Databricks notebook source
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import plotly.express as px

from datetime import datetime
from pyspark.sql import functions as fun

# COMMAND ----------

# MAGIC %md
# MAGIC Read all tables last modified times and load to new freshness_measure table. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Read sample df for last modified time for a table

# COMMAND ----------

df = sqlContext.sql("DESCRIBE DETAIL rtumati.diamonds_copy")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC Add measured timestamp details to ensure "measure at" can be tracked 

# COMMAND ----------

update_time = df.select(["id", "name", "lastModified"]).withColumn("measureAt", fun.lit(datetime.utcnow()))
display(update_time)

# COMMAND ----------

# MAGIC %md
# MAGIC Write to table

# COMMAND ----------


update_time.write.saveAsTable("rtumati.freshness_measure", overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Read table schema to Mock data 

# COMMAND ----------


df_update_times=spark.sql("select * from rtumati.freshness_measure")
df_update_times.show()
update_time_schema= df_update_times.schema
print(update_time_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Mock Data to create historic data updates.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import expr
from datetime import datetime, timedelta
from pyspark.sql import functions as fun
import random

last_modified = '2024-04-01T00:00:00.000+00:00'
measure_at = '2024-04-01T00:00:00.000+00:00'

# Define schema for DataFrame
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("lastModified", TimestampType(), True),
    StructField("measureAt", TimestampType(), True)
])

last_modified_init = '2024-02-1 0:01:00'
last_modified_end = '2024-04-21 0:01:00'
measure_at_init = '2024-02-1 11:01:00'

last_modified = datetime.strptime(last_modified_init, '%Y-%m-%d %H:%M:%S')
last_modified_end = datetime.strptime(last_modified_end, '%Y-%m-%d %H:%M:%S')
measure_at= datetime.strptime(measure_at_init, '%Y-%m-%d %H:%M:%S')

print(last_modified)
print(last_modified_end)
days_difference = (last_modified_end - last_modified).days
print(days_difference)
i= 1
while i < 76:
  random_minutes = random.randint(1, 59)
  random_min = random.randint(1, 4)
  if i in (40, 41):
    last_modified_upd =last_modified + timedelta(days=39, minutes = random_minutes)
  else:
    last_modified_upd =last_modified + timedelta(days=i, minutes = random_minutes)
  measure_at_upd =measure_at + timedelta(days=i, minutes = random_min)
  print(last_modified_upd)
  print(measure_at_upd)
  df = spark.createDataFrame([( 
  'f958a407-3e27-4040-a726-2668f37dd0c4', 
  'spark_catalog.rtumati.diamonds_copy', 
  last_modified_upd, 
  measure_at_upd
  )], schema=schema)

  df.write.format("delta").mode("append").saveAsTable("rtumati.freshness_measure")
  i = i+1
    # Insert data into the table




# COMMAND ----------

# MAGIC %md
# MAGIC Introduce Anamolies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- introduce anamolies
# MAGIC update  rtumati.freshness_measure 
# MAGIC set lastModified = (select lastModified  from rtumati.freshness_measure where date(measureAt) = '2024-04-1')
# MAGIC where date(measureAt) = '2024-04-5';
# MAGIC
# MAGIC %sql
# MAGIC INSERT INTO rtumati.freshness_measure (id, name, lastModified, measureAt)
# MAGIC VALUES 
# MAGIC ('f958a407-3e27-4040-a726-2668f37dd0c4', 'spark_catalog.rtumati.diamonds_copy', '2024-04-2T17:16:30.000+00:00', '2024-04-21T17:16:52.055+00:00'),
# MAGIC ('f958a407-3e27-4040-a726-2668f37dd0c4', 'spark_catalog.rtumati.diamonds_copy', '2024-04-20T17:16:15.000+00:00', '2024-04-20T17:16:52.055+00:00'),
# MAGIC ('f958a407-3e27-4040-a726-2668f37dd0c4', 'spark_catalog.rtumati.diamonds_copy', '2024-04-19T17:16:20.000+00:00', '2024-04-19T17:16:52.055+00:00');
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

time_series = sqlContext.sql("""select distinct
              measureAt,
              lastModified,
              1 as val
              from rtumati.freshness_measure
              where name = 'spark_catalog.rtumati.diamonds_copy'
              order by measureAt ASC
              limit 400""").toPandas()
#time = spark.sql("select distinct measureAt, lastModified, 1 as Val from rtumati.diamonds_update_times where name = 'spark_catalog.rtumati.diamonds_copy' order by measureAt ASC limit 400")
#time.show()
time_series.lastModified = pd.to_datetime(time_series.lastModified)
time_series.measureAt = pd.to_datetime(time_series.measureAt)

print(time_series)


# COMMAND ----------

px.scatter(x=time_series.lastModified, y=time_series.val)


# COMMAND ----------

px.scatter(x=time_series.measureAt, y=time_series.val)

# COMMAND ----------

differences = time_series.assign(delaySecods=lambda x:(x.lastModified - time_series.shift(periods=1, axis="rows").lastModified/ np.timedelta64(1, 's')))

# COMMAND ----------

import numpy as np

# Assuming 'lastModified' is a datetime column in your DataFrame
time_series['delaySeconds'] = (time_series['lastModified'] - time_series['lastModified'].shift(1)) / np.timedelta64(1, 's')
time_series['delayMinutes'] = (time_series['lastModified'] - time_series['lastModified'].shift(1)) / np.timedelta64(1, 'm')
print(time_series)
#differences = time_series.assign(delaySeconds=lambda x: (x['lastModified'] - time_series.shift(periods=1)['lastModified']) / np.timedelta64(1, 's'))

differences = time_series.assign(delayMinutes=lambda x: (x['lastModified'] - time_series.shift(periods=1)['lastModified']) / np.timedelta64(1, 'm'))
print(differences)



# COMMAND ----------

print(differences[differences['delaySeconds'] > 86400])


# COMMAND ----------

differences = differences.loc[~(differences.delaySeconds==0)].dropna().reset_index()
print(differences)

# COMMAND ----------

print(differences[differences['delaySeconds'] > 100000])

# COMMAND ----------

spikes = differences[differences['delaySeconds'] > 80000]

# Plot the bars
fig = px.bar(x=spikes['lastModified'], y=spikes ['delaySeconds'] )
fig.show()

# COMMAND ----------

px.bar(x=differences.lastModified, y = differences.delaySeconds)

# COMMAND ----------

mlflow.sklearn.autolog()

# COMMAND ----------

#from sklearn.neighbors import LocalOutlierFactor
#clf = LocalOutlierFactor(n_neighbors=10)
#predictions = clf.fit_predict(differences[("delaySeconds")]).values
#autolog_run = mlflow.last_active_run()

from sklearn.neighbors import LocalOutlierFactor

# Reshape the input data to have 2D shape
X = differences[["delaySeconds"]].values.reshape(-1, 1)

# Initialize and fit the LocalOutlierFactor model
clf = LocalOutlierFactor(n_neighbors=10)
predictions = clf.fit_predict(X)
autolog_run = mlflow.last_active_run()

# Now you can use 'predictions' for further analysis


# COMMAND ----------

clf.negative_outlier_factor_

# COMMAND ----------

spikes = differences[differences['delaySeconds'] > 80000]

# Plot the bars
fig = px.bar(x=spikes['lastModified'], y=spikes ['delaySeconds'] )
for i, v in enumerate(predictions):
  if v ==-1: fig.add_vline(x=differences.iloc[i].lastModified, line_color="red", opacity=0.4)
fig.show()

# COMMAND ----------

from pyspark.sql import Row

df_dia= spark.sql("select * from rtumati.diamonds_copy")
df_dia.printSchema()
new_row = Row(_c0='671', carat='new_carat', cut='new_cut', color='new_color',
              clarity='new_clarity', depth='new_depth', table='new_table', price='new_price',
              x='new_x', y='new_y', z='new_z')

# Convert the new row to a DataFrame
new_df = spark.createDataFrame([new_row], df_dia.schema)

# Concatenate the original DataFrame with the new DataFrame
df_with_new_row = df_dia.union(new_df)

df_with_new_row.write.format("delta").mode("append").saveAsTable("rtumati.diamonds_copy")

# COMMAND ----------

table_metadata = spark.sql("DESCRIBE EXTENDED rtumati.diamonds_copy").collect()

# Print the metadata
for row in table_metadata:
    print(row)
