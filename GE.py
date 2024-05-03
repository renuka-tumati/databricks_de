# Databricks notebook source
pip install great_expectations

# COMMAND ----------

import great_expectations as ge
import pandas as pd

# Create a sample DataFrame (replace this with your actual data)
data = {
    'user_id': [1, 2, 3, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'Charlie', 'David', 'Eve']
}

df = pd.DataFrame(data)

# Initialize a Great Expectations context
context = ge.data_context.DataContext()

# Create a Great Expectations Expectation Suite
suite = context.create_expectation_suite("my_expectations")

# Add an expectation for the 'user_id' column
suite.expect_column_values_to_be_unique("user_id")

# Validate the DataFrame against the expectation suite
validator = context.get_validator(df, suite)
results = validator.validate()
print(results)

# Check if the validation passed
if results["success"]:
    print("Column values are unique.")
else:
    print("Duplicate values found in the column.")

# Optionally, you can also set a threshold for uniqueness using the 'mostly' parameter.
# For example, to consider the expectation successful if at least 90% of values are unique:
# suite.expect_column_values_to_be_unique("user_id", mostly=0.9)

