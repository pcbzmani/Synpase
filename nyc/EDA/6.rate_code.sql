-- Databricks notebook source
USE nyctaxi;

SELECT rate_code_id,
rate_code
    FROM OPENROWSET(
        BULK 'rate_code.json',
        DATA_SOURCE = 'fsnyctxi_raw',
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
    );

-- multiline
SELECT rate_code_id,
rate_code
    FROM OPENROWSET(
        BULK 'rate_code_multi_line.json',
        DATA_SOURCE = 'fsnyctxi_raw',
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
    );
