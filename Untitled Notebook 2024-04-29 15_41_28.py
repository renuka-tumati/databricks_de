# Databricks notebook source
# Mount S3 bucket
s3link = "https://us-east-1.console.aws.amazon.com/s3/buckets/rtumatidatabricks?region=us-east-1&bucketType=general&tab=objects"
dbutils.fs.mount(source = "s3link", mount_point = "/mnt/rtdb")



