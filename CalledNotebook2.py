# Databricks notebook source
import datetime
# Set the Python variable
x = str(datetime.date.today())
dbutils.widgets.text("max_date", x)

print("Called Notebook2 to set date")
