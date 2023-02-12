-- Databricks notebook source
USE nyc_taxi_ldw
GO

IF object_id('silver.trip_type') is not NULL 
    DROP EXTERNAL TABLE silver.trip_type
GO

CREATE EXTERNAL TABLE silver.trip_type
WITH(
    LOCATION = 'transformed/trip_type',
    DATA_SOURCE = nyc_taxi_src,
    FILE_FORMAT = parquet_file_format
)
AS
SELECT * FROM bronze.trip_type
GO


