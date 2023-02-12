-- Databricks notebook source


IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'parquet_format') 
	CREATE EXTERNAL FILE FORMAT parquet_format
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'nyxtaxids') 
	CREATE EXTERNAL DATA SOURCE nyxtaxids
	WITH (
		LOCATION = 'abfss://fsnyctxi@dlnyctxi.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE stage.trip_data_green_agg (
	[PULocationID] int,
	[DOLocationID] int,
	[total_trip_count] bigint,
	[total_fare_amount] float
	)
	WITH (
	LOCATION = 'gold/trip_data_green_agg/**',
	DATA_SOURCE = nyxtaxids,
	FILE_FORMAT = parquet_format
	)
GO


SELECT TOP 100 * FROM stage.trip_data_green_agg
GO

CREATE SCHEMA dwh
GO

CREATE TABLE dwh.trip_data_green
WITH(
	CLUSTERED COLUMNSTORE INDEX,
	DISTRIBUTION = ROUND_ROBIN
)AS SELECT * FROM stage.trip_data_green_agg
