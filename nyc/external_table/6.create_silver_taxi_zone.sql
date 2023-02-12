-- Databricks notebook source
USE nyc_taxi_ldw
GO

IF object_id('silver.taxi_zone') is not NULL
  drop EXTERNAL TABLE silver.taxi_zone
GO


CREATE EXTERNAL TABLE silver.taxi_zone
WITH(
    LOCATION = 'transformed/taxi_zone',
    DATA_SOURCE = nyc_taxi_src,
    FILE_FORMAT = parquet_file_format
)
AS
 SELECT location_id,
 borough,
 zone,
 service_zone
  FROM bronze.taxi_zone
GO

SELECT * FROM silver.taxi_zone;


