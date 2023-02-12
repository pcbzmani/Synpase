-- Databricks notebook source
USE nyc_taxi_ldw
GO

IF object_id('silver.calendar') is not NULL 
    DROP EXTERNAL TABLE silver.calendar
GO

CREATE EXTERNAL TABLE silver.calendar
WITH(
    LOCATION = 'transformed/calendar',
    DATA_SOURCE = nyc_taxi_src,
    FILE_FORMAT = parquet_file_format
)
AS
SELECT * FROM silver.calendar
GO


