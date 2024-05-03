# Databricks notebook source
spark.sql("select * from main.rtumati.user_activity").show()

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("QueryMetricsAnalyzer") \
    .getOrCreate()

# Function to retrieve query execution metrics for a given Delta table or materialized view
def get_query_metrics(table_name):
    # SQL query to retrieve query execution metrics from the history
    print(f"{table_name}")
    query = f"""
        SELECT
            qh.job_id,
            qh.job_description,
            qh.start_time,
            qh.end_time,
            qh.execution_time,
            qh.result_size,
            qh.output_records,
            qh.executor_id,
            qh.executor_run_time,
            qh.executor_cpu_time,
            qh.executor_shuffle_read,
            qh.executor_shuffle_write,
            qh.result_prediction
        FROM
            (SELECT
                query_id,
                job_id,
                job_description,
                start_time,
                end_time,
                execution_time,
                result_size,
                output_records,
                executor_id,
                executor_run_time,
                executor_cpu_time,
                executor_shuffle_read,
                executor_shuffle_write,
                result_prediction,
                row_number() OVER (PARTITION BY query_id ORDER BY start_time DESC) as rownum
            FROM
                history
            WHERE
                query_id IN (SELECT query_id FROM table_history WHERE table_name = '{table_name}')
            ) qh
        WHERE
            qh.rownum = 1
    """
    print(f"{query}")
    # Execute the SQL query and retrieve the results
    query_metrics_df = spark.sql(query)
    
    return query_metrics_df

# Example usage: Get query execution metrics for a given Delta table or materialized view
table_name = "main.rtumati.user_activity"
query_metrics = get_query_metrics(table_name)

# Display the query execution metrics
query_metrics.show()

