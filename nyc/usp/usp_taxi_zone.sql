-- Databricks notebook source
use nyc_taxi_ldw
GO

CREATE OR ALTER PROCEDURE silver.taxi_zoe_silver_p
AS
BEGIN
    IF object_id('silver.taxi_zone') is not NULL
        drop EXTERNAL TABLE silver.taxi_zone;

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
    FROM bronze.taxi_zone;
END
