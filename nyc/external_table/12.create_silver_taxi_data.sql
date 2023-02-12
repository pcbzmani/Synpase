-- Databricks notebook source
USE nyc_taxi_ldw
GO

DROP VIEW IF EXISTS silver.vw_green_trip_data
GO

CREATE VIEW silver.vw_green_trip_data
AS
SELECT
    nyctaxi_par.filepath(1) as year,
    nyctaxi_par.filepath(2) as month,
    nyctaxi_par.*
FROM
    OPENROWSET(
        BULK 'raw/trip_data_green_parquet/year=*/month=*/',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'PARQUET'
    ) AS nyctaxi_par
GO

SELECT DISTINCT  year, month from silver.vw_green_trip_data;
