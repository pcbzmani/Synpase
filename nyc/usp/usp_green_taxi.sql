-- Databricks notebook source
USE nyc_taxi_ldw
GO


CREATE OR ALTER PROCEDURE silver.usp_green_taxi_data
@year VARCHAR(4),
@month VARCHAR(2)
AS
BEGIN
    DECLARE @create_sql_start NVARCHAR(MAX),
            @drop_sql_test NVARCHAR(MAX);
    
    SET @create_sql_start = 
    'CREATE EXTERNAL TABLE silver.trip_data_' +@year +'_' + @month +
    ' WITH( 
        DATA_SOURCE = nyc_taxi_src,
        LOCATION = ''silver/trip_data_green/year=' + @year + '/month=' + @month +''',
        FILE_FORMAT = parquet_file_format
    ) AS
    SELECT * FROM bronze.vw_green_trip_data
    WHERE year = ''' + @year + '''
    and month = ''' + @month + '''';

    print(@create_sql_start)

    EXEC sp_executesql @create_sql_start;

    SET @drop_sql_test =
        'DROP EXTERNAL TABLE silver.trip_data_' +@year +'_' + @month;
    
    print(@drop_sql_test)
    EXEC sp_executesql @drop_sql_test;
    

END
