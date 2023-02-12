-- Databricks notebook source
use nyc_taxi_ldw
GO


DROP VIEW bronze.payment_type
GO

CREATE VIEW bronze.payment_type
AS 
SELECT payment_type,
    payment_desc_value
    FROM OPENROWSET(
        BULK 'raw/payment_type_array.json',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a'
    )WITH(
        jsonres NVARCHAR(MAX)
    )as payment_typ
    CROSS APPLY OPENJSON (
        jsonres
    )WITH(
        payment_type smallint,
        payment_type_desc NVARCHAR(MAX) as JSON
    )CROSS APPLY OPENJSON(
        payment_type_desc
    )WITH(
        sub_type smallint,
        payment_desc_value VARCHAR(20) '$.value'
    )
GO

-- SELECT * FROM bronze.payment_type;
DROP VIEW bronze.rate_code
GO

CREATE VIEW bronze.rate_code
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

SELECT * FROM bronze.rate_code;


DROP VIEW bronze.vw_green_trip_data
GO

CREATE VIEW bronze.vw_green_trip_data
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

SELECT * from bronze.vw_green_trip_data;
