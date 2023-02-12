-- Databricks notebook source
USE nyc_taxi_ldw
GO

IF OBJECT_ID('silver.payment_type') IS NOT NULL
    DROP EXTERNAL TABLE silver.payment_type
GO

CREATE EXTERNAL TABLE silver.payment_type
WITH(
    LOCATION = 'transformed/payment_type',
    DATA_SOURCE = nyc_taxi_src,
    FILE_FORMAT = parquet_file_format
)
AS
SELECT * from bronze.payment_type
GO

SELECT * FROM silver.payment_type;

EXEC silver.usp_green_taxi_data '2020','01';
EXEC silver.usp_green_taxi_data '2020','02';
EXEC silver.usp_green_taxi_data '2020','03';
EXEC silver.usp_green_taxi_data '2020','04';
EXEC silver.usp_green_taxi_data '2020','05';
EXEC silver.usp_green_taxi_data '2020','06';
EXEC silver.usp_green_taxi_data '2020','07';
EXEC silver.usp_green_taxi_data '2020','08';
EXEC silver.usp_green_taxi_data '2020','09';
EXEC silver.usp_green_taxi_data '2020','10';
EXEC silver.usp_green_taxi_data '2020','11';
EXEC silver.usp_green_taxi_data '2020','12';
EXEC silver.usp_green_taxi_data '2021','01';
EXEC silver.usp_green_taxi_data '2021','02';
EXEC silver.usp_green_taxi_data '2021','03';
EXEC silver.usp_green_taxi_data '2021','04';
EXEC silver.usp_green_taxi_data '2021','05';
EXEC silver.usp_green_taxi_data '2021','06';
EXEC silver.usp_green_taxi_data '2020','01';
EXEC silver.usp_green_taxi_data '2020','01';
EXEC silver.usp_green_taxi_data '2020','01';


