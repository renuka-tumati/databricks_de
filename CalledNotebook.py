# Databricks notebook source
import datetime
max_date2 = str(datetime.date.today())
spark.conf.set("abc.max_dt2", max_date2)

print("Called Notebook to set date")
