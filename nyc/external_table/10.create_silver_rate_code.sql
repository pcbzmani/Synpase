-- Databricks notebook source
use nyc_taxi_ldw
GO

IF object_id('silver.rate_code') is not NULL 
    DROP EXTERNAL TABLE silver.rate_code
GO

CREATE EXTERNAL TABLE silver.rate_code
WITH(
    LOCATION = 'transformed/rate_code',
    DATA_SOURCE = nyc_taxi_src,
    FILE_FORMAT = parquet_file_format
)
AS
SELECT rate_code_id,
rate_code
    FROM OPENROWSET(
        BULK 'raw/rate_code.json',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0b'
    )WITH(
        jsonres NVARCHAR(MAX)
    )as rate_code
    CROSS APPLY OPENJSON (
        jsonres
    )WITH(
        rate_code_id smallint,
        rate_code varchar(50) 
    )
GO

SELECT * from silver.rate_code;
