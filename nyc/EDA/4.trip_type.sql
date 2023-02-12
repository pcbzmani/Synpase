-- Databricks notebook source
use nyctaxi;

SELECT 
    *
    FROM
    OPENROWSET (
        BULK 'trip_type.tsv',
        DATA_SOURCE = 'fsnyctxi_raw',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = '\t',
        PARSER_VERSION = '2.0'
    )WITH (
        trip_type TINYINT 1,
        trip_type_desc VARCHAR(50)
    )
     AS trip_type;

sp_describe_first_result_set N'SELECT 
    *
    FROM
    OPENROWSET (
        BULK ''trip_type.tsv'',
        DATA_SOURCE = ''fsnyctxi_raw'',
        FORMAT = ''CSV'',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ''\t'',
        PARSER_VERSION = ''2.0''
    ) AS trip_type'

