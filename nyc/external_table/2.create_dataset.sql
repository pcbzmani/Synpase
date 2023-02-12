-- Databricks notebook source
use nyc_taxi_ldw
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name= 'nyc_taxi_src')
    CREATE EXTERNAL DATA SOURCE nyc_taxi_src
    WITH (
        LOCATION ='https://dlnyctxi.dfs.core.windows.net/fsnyctxi'
    );

