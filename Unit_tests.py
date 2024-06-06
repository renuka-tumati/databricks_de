# Databricks notebook source
import pandas as pd
import unittest

def transform_data_sum(column):
    # Logic to transform the extracted data
    # For demonstration, let's assume aggregation by summing integers in each cell of the DataFrame column
    aggregated_data = column.sum()
    return aggregated_data


def transform_data_avg(column,count):
    # Logic to transform the extracted data
    # For demonstration, let's assume aggregation by summing integers in each cell of the DataFrame column
    avg_data = column.sum()/count
    return avg_data

class TestTransformDataCol(unittest.TestCase):
    def test_transform_data_sum(self):
        # Arrange
        source_data = pd.DataFrame({"col1": [10, 20, 30, 40, 50, 60, 70, 80, 90]})  # Sample input DataFrame
        
        # Act
        transformed_data = transform_data_sum(source_data["col1"])
        sum_of_column = source_data['col1'].sum()
        
        # Assert
        self.assertEqual(transformed_data, sum_of_column)  # Corrected to match the function's expected behavior


    def test_transform_data_avg(self):
        # Arrange
        source_data = pd.DataFrame({"col1": [10, 20, 30, 40, 50, 60, 70, 80, 90]})  # Sample input DataFrame
        count = len(source_data)  # Number of elements in the DataFrame column
        
        # Act
        transformed_data = transform_data_avg(source_data["col1"], count)
        expected_avg = source_data["col1"].sum() / count
        
        # Assert
        self.assertEqual(transformed_data, expected_avg)  # Check if the calculated average is correct
    
    def test_transform_data_avg_with_custom_count(self):
        # Arrange
        source_data = pd.DataFrame({"col1": [10, 20, 30, 40, 50, 60, 70, 80, 90]})  # Sample input DataFrame
        count = 5  # Custom count that is not equal to the length of the column
        
        # Act
        transformed_data = transform_data_avg(source_data["col1"], count)
        expected_avg = source_data["col1"].sum() / count
        
        # Assert
        self.assertEqual(transformed_data, expected_avg)  # Check if the calculated average with custom count is correct
    
    def test_transform_data_avg_with_zero_count(self):
        # Arrange
        source_data = pd.DataFrame({"col1": [10, 20, 30, 40, 50, 60, 70, 80, 90]})  # Sample input DataFrame
        count = 0  # Count is zero to test division by zero handling
        
        # Act & Assert
        with self.assertRaises(ZeroDivisionError):
            transform_data_avg(source_data["col1"], count)  # Check if division by zero raises an error
    
    def test_transform_data_avg_with_empty_column(self):
        # Arrange
        source_data = pd.DataFrame({"col1": []})  # Empty DataFrame column
        count = 1  # Count is non-zero to test handling of empty column
        
        # Act
        transformed_data = transform_data_avg(source_data["col1"], count)
        expected_avg = 0  # Average of an empty column is zero
        
        # Assert
        self.assertEqual(transformed_data, expected_avg)  # Check if the calculated average is correct for empty column

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)

        




