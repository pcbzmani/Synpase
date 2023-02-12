-- Databricks notebook source
USE nyc_taxi_ldw
GO

CREATE OR ALTER PROCEDURE gold.credit_card_campaign
@year VARCHAR(4),
@month VARCHAR(2)
AS
BEGIN
    DECLARE @select_query NVARCHAR(MAX),
            @drop_sql_test NVARCHAR(MAX);

    SET @select_query =
            'CREATE EXTERNAL TABLE gold.trip_data_' +@year +'_' + @month +
             ' WITH( 
                DATA_SOURCE = nyc_taxi_src,
                LOCATION = ''gold/trip_data_green/year=' + @year + '/month=' + @month +''',
                FILE_FORMAT = parquet_file_format
               ) AS 
          SELECT 
            td.year,
            td.month,
            CAST(td.lpep_pickup_datetime as date) as date_of_trip,
            cal.day_name,
            tz.borough,
            SUM(CASE
                WHEN pt.payment_desc_value = ''Credit card'' THEN 1
                ELSE 0
                END) as credit_card_trans,
            SUM(CASE
                WHEN pt.payment_desc_value = ''Cash'' THEN 1
                ELSE 0
                END) as cash_trans, 
            SUM(CASE
                WHEN tt.trip_type_desc = ''Dispatch'' THEN 1
                ELSE 0
                END) as dispatch_count,
            SUM(CASE
                WHEN tt.trip_type_desc = ''Street-hail'' THEN 1
                ELSE 0
                END) as street_hail_count, 
            CASE 
                WHEN cal.day_name IN (''Saturday'',''Sunday'') THEN ''Y'' 
                ELSE ''N''
                END AS weekend_ind,
            sum(DATEDIFF(MINUTE , CAST(td.lpep_pickup_datetime as time),  CAST(td.lpep_dropoff_datetime as time))) as trip_duration,
            SUM(trip_distance) as trip_distance,
            SUM(fare_amount) as fare_amount
            FROM silver.vw_green_trip_data td
            JOIN silver.calendar cal 
            ON CAST(td.lpep_pickup_datetime AS date) = cal.date
            JOIN silver.taxi_zone tz
            ON tz.location_id = td.PULocationID
            JOIN silver.payment_type pt 
            ON td.payment_type = pt.payment_type
            JOIN silver.trip_type tt 
            ON td.trip_type = tt.trip_type
        WHERE td.year = ''' + @year + ''' 
        and td.month = ''' + @month + ''' 
            GROUP BY 
            td.year,
            td.month,
            CAST(td.lpep_pickup_datetime as date),
            cal.day_name,
            tz.borough'
    
    print(@select_query)

    EXEC sp_executesql @select_query;

    SET @drop_sql_test =
        'DROP EXTERNAL TABLE gold.trip_data_' +@year +'_' + @month;
    
    print(@drop_sql_test)
    EXEC sp_executesql @drop_sql_test;
END
