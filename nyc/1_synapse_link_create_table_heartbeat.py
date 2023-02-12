# Databricks notebook source
# MAGIC %%sql
# MAGIC 
# MAGIC -- Create a Spark table over Cosmos DB analytical store
# MAGIC -- To select a preferred list of regions in a multi-region Cosmos DB account, add spark.cosmos.preferredRegions '<Region1>,<Region2>' in the config options
# MAGIC 
# MAGIC create table nyc_taxi_ldw_spark.heartbeat using cosmos.olap options (
# MAGIC     spark.synapse.linkedService 'ls_cosmos_db_nyc_taxi_data',
# MAGIC     spark.cosmos.container 'Heartbeat'
# MAGIC )

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM nyc_taxi_ldw_spark.heartbeat
