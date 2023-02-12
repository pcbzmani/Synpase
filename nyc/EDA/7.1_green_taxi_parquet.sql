-- Databricks notebook source
use nyctaxi
-- This is auto-generated code
SELECT
    nyctaxi_par.filepath(1) as year,
    nyctaxi_par.filepath(2) as month,
     COUNT(*)
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=*/month=*/',
        DATA_SOURCE = 'fsnyctxi_raw',
        FORMAT = 'PARQUET'
    ) AS nyctaxi_par
where nyctaxi_par.filepath(1) = '2021'
and nyctaxi_par.filepath(2) in ('01','02','03','04')
group by nyctaxi_par.filepath(1),nyctaxi_par.filepath(2)

