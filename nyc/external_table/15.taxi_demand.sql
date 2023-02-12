-- Databricks notebook source
USE nyc_taxi_ldw
GO

SELECT 
    td.year,
    td.month,
    tz.borough,
    CAST(td.lpep_pickup_datetime as date) as trip_date,
    sum(DATEDIFF(MINUTE , td.lpep_pickup_datetime,  td.lpep_dropoff_datetime )) as trip_duration,
    cal.day_name,
    CASE 
        WHEN cal.day_name IN ('Saturday','Sunday') THEN 'Y' 
        ELSE 'N'
        END AS weekend_ind,
    SUM(CASE
        WHEN tt.trip_type_desc = 'Dispatch' THEN 1
        ELSE 0
        END) as dispatch_count,
    SUM(CASE
        WHEN tt.trip_type_desc = 'Street-hail' THEN 1
        ELSE 0
        END) as street_hail_count, 
    SUM(trip_distance) as trip_distance,
    SUM(total_amount) as total_amount
    FROM silver.vw_green_trip_data td
    JOIN silver.calendar cal 
    ON CAST(td.lpep_pickup_datetime AS date) = cal.date
    JOIN silver.taxi_zone tz
    ON tz.location_id = td.PULocationID
    JOIN silver.trip_type tt 
    on td.trip_type = tt.trip_type
WHERE td.year = '2021'
and td.month = '01'
    GROUP BY 
    td.year,
    td.month,
    CAST(td.lpep_pickup_datetime as date),
    cal.day_name,
    tz.borough;


SELECT *  FROM silver.vw_green_trip_data td;

SELECT * from silver.taxi_zone;

SELECT * from silver.trip_type tt

SELECT * from silver.calendar;



EXEC silver.raw_silver_load 'taxi_zone';

