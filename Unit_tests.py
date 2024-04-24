# Databricks notebook source
import pandas as pd
import unittest

def transform_data_col(column):
    # Logic to transform the extracted data
    # For demonstration, let's assume aggregation by summing integers in each cell of the DataFrame column
    aggregated_data = column.sum()
    return aggregated_data

class TestTransformDataCol(unittest.TestCase):
    def test_transform_data_col(self):
        # Arrange
        source_data = pd.DataFrame({"col1": [10, 20, 30, 40, 50, 60, 70, 80, 90]})  # Sample input DataFrame
        
        # Act
        transformed_data = transform_data_col(source_data["col1"])
        sum_of_column = source_data['col1'].sum()
        
        # Assert
        self.assertEqual(transformed_data, sum_of_column+10)  # Sum of numbers in each cell of the input DataFrame
if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)

# COMMAND ----------

source_data = pd.DataFrame({"col5": [10, 20, 30]}) 
sum = transform_data_col(source_data)
print(sum)

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum 

def transform_data_column(column):
    # Logic to transform the extracted data
    # For demonstration, let's assume aggregation by summing integers in each cell of the DataFrame column
    aggregated_data = column.select(sum(column[0]))
    print(aggregated_data)
    return aggregated_data

class TestTransformDataColumn(unittest.TestCase):
    def test_transform_data_column(self):
        
        # Create a sample DataFrame
        data = [(10,), (20,), (30,), (40,), (50,), (60,), (70,), (80,), (90,)]
        df = spark.createDataFrame(data, ['col1'])
        
        # Act
        transformed_data = transform_data_column(df)
        sum_of_column = df.select(sum('col1'))

        print(sum_of_column)
        print(transformed_data)
        
        # Assert
        # self.assertEqual(transformed_data, sum_of_column + 10)  # Sum of numbers in each cell of the input DataFrame

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)


# COMMAND ----------

data = [(10,), (20,), (30,), (40,), (50,), (60,), (70,), (80,), (90,)]
        df = spark.createDataFrame(data, ['col1'])
