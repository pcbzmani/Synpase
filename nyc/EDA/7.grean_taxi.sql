-- Databricks notebook source
-- This is auto-generated code
SELECT
    green_taxi.filepath(1) as year,
    green_taxi.filepath(2) as month,
    count(*)
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*',
        DATA_SOURCE = 'fsnyctxi_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS green_taxi
WHERE  green_taxi.filepath(1) = '2020'
and  green_taxi.filepath(2) in ('05','06')
group by green_taxi.filepath(1), green_taxi.filepath(2)
order by green_taxi.filepath(1), green_taxi.filepath(2)

EXEC sp_describe_first_result_set N'SELECT
    top 100 *
FROM
    OPENROWSET(
        BULK ''trip_data_green_csv/year=*/month=*/*'',
        DATA_SOURCE = ''fsnyctxi_raw'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE
    ) AS green_taxi'
