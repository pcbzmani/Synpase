# Databricks notebook source
# Read from Cosmos DB analytical store into a Spark DataFrame and display 10 rows from the DataFrame
# To select a preferred list of regions in a multi-region Cosmos DB account, add .option("spark.cosmos.preferredRegions", "<Region1>,<Region2>")

df = spark.read\
    .format("cosmos.olap")\
    .option("spark.synapse.linkedService", "ls_cosmos_db_nyc_taxi_data")\
    .option("spark.cosmos.container", "Heartbeat")\
    .load()

display(df.limit(10))
