# Databricks notebook source
from pyspark.sql import SparkSession

# Create a Spark Session
spark = SparkSession.builder \
    .appName("My-test-app") \
    .config("spark.some.config.option", "config-value") \
    .getOrCreate()
    


# COMMAND ----------

# Data for the DataFrame
data = [("Alice", 34), ("Bob", 45)]

# Create a DataFrame
df = spark.createDataFrame(data, ["name", "age"])

# Show the DataFrame
df.show()

# Register the DataFrame as a global temporary view (with a name, e.g., "people")
df.createOrReplaceGlobalTempView("people")

# SQL query for ordering by age in descending order
Asc = spark.sql("SELECT * FROM global_temp.people ORDER BY age DESC")

# Show the results
Asc.show()

# Stop the Spark session
spark.stop()

