# Databricks notebook source
# MAGIC %md
# MAGIC ## Trip Data Aggregation 
# MAGIC ### Group By Columns
# MAGIC 1. year
# MAGIC 2. Month
# MAGIC 3. Pickup Location ID
# MAGIC 4. Drop Off Location ID
# MAGIC 
# MAGIC ### Aggregated Columns
# MAGIC 1. Total Trip Count
# MAGIC 2. Total Fare Amount
# MAGIC 
# MAGIC ### Purpose of the notebook
# MAGIC 
# MAGIC Demonstrate the integration between Spark Pool and Serverless SQL Pool
# MAGIC 
# MAGIC 1. Create the aggregated table in Spark Pool
# MAGIC 2. Access the data from Serverless SQL Pool 

# COMMAND ----------

#Set the folder paths so that it can be used later. 
bronze_folder_path = 'abfss://nyc-taxi-data@synapsecoursedl.dfs.core.windows.net/raw'
silver_folder_path = 'abfss://nyc-taxi-data@synapsecoursedl.dfs.core.windows.net/silver'
gold_folder_path = 'abfss://nyc-taxi-data@synapsecoursedl.dfs.core.windows.net/gold'

# COMMAND ----------

#Set the spark config to be able to get the partitioned columns year and month as strings rather than integers
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

# COMMAND ----------

# MAGIC %%sql
# MAGIC 
# MAGIC -- Create database to which we are going to write the data
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS nyc_taxi_ldw_spark
# MAGIC LOCATION 'abfss://nyc-taxi-data@synapsecoursedl.dfs.core.windows.net/gold';

# COMMAND ----------

# Read the silver data to be processed. 
trip_data_green_df = spark.read.parquet(f"{silver_folder_path}/trip_data_green") 

# COMMAND ----------

# Perform the required aggregations
# 1. Total trip count
# 2. Total fare amount
from pyspark.sql.functions import *

trip_data_green_agg_df = trip_data_green_df \
                        .groupBy("year", "month", "pu_location_id", "do_location_id") \
                        .agg(count(lit(1)).alias("total_trip_count"),
                        round(sum("fare_amount"), 2).alias("total_fare_amount"))

# COMMAND ----------

# Write the aggregated data to the gold table for consumption

trip_data_green_agg_df.write.mode("overwrite").partitionBy("year", "month").format("parquet").saveAsTable("nyc_taxi_ldw_spark.trip_data_green_agg")
