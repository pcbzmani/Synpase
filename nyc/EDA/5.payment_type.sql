-- Databricks notebook source

use nyctaxi;

SELECT cast(JSON_VALUE(jsonres, '$.payment_type') AS INT) as payment_typ,
       cast(JSON_VALUE(jsonres, '$.payment_type_desc') AS VARCHAR(15)) as payment_typ_des
    FROM OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'fsnyctxi_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a'
    )WITH(
        jsonres VARCHAR(MAX)
    ) as payment_typ;


    -- open json  
SELECT payment_type,
description
    FROM OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'fsnyctxi_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
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
        description varchar(50) '$.payment_type_desc'
    ) ;


-- READ Array Json
SELECT payment_type,
    payment_desc_value
    FROM OPENROWSET(
        BULK 'payment_type_array.json',
        DATA_SOURCE = 'fsnyctxi_raw',
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
    ) ;

