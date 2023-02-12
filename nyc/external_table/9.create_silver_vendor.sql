-- Databricks notebook source
USE nyc_taxi_ldw
GO

IF object_id('silver.vendor') is not NULL 
    DROP EXTERNAL TABLE silver.vendor
GO

CREATE EXTERNAL TABLE silver.vendor
WITH(
    LOCATION = 'transformed/vendor',
    DATA_SOURCE = nyc_taxi_src,
    FILE_FORMAT = parquet_file_format
)
AS
SELECT * FROM bronze.vendor
GO

