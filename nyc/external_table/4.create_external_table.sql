-- Databricks notebook source
use nyc_taxi_ldw
GO

-- IF OBJECT_ID('silver.trip_data_2020_01') IS NOT NULL
--     DROP EXTERNAL TABLE silver.trip_data_2020_01

IF NOT EXISTS (SELECT * FROM sys.external_tables WHERE name = 'taxi_zone')
    CREATE EXTERNAL TABLE bronze.taxi_zone
    (
        location_id SMALLINT ,
        borough VARCHAR(15) ,
        zone VARCHAR(50) ,
        service_zone VARCHAR(15)  
    )
    WITH (
        LOCATION = 'raw/taxi_zone.csv',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = csv_file_format_pv1,
        REJECT_VALUE = 10,
        REJECTED_ROW_LOCATION = 'rejection/taxi_zone'

    )
    GO

-- SELECT * FROM bronze.taxi_zone;

IF NOT EXISTS (SELECT * FROM sys.external_tables WHERE name = 'calendar')
    CREATE EXTERNAL TABLE bronze.calendar(
        date_key INT,
        date DATE ,
        year smallint,
        month TINYINT ,
        day TINYINT ,
        day_name varchar(15) ,
        day_of_year smallint ,
        week_of_month TINYINT ,
        week_of_year TINYINT ,
        month_name varchar(15) ,
        year_month INT ,
        year_week int
    )WITH (
        LOCATION = 'raw/calendar.csv',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = csv_file_format
    );

-- SELECT * FROM bronze.calendar

IF NOT EXISTS (SELECT * FROM sys.external_tables WHERE name = 'vendor')
    CREATE EXTERNAL TABLE bronze.vendor(
        vendor_id int,
        vendor_name VARCHAR(50)
    )WITH(
        LOCATION = 'raw/vendor.csv',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = csv_file_format
    );

-- SELECT * from bronze.vendor;

IF NOT EXISTS (SELECT * FROM sys.external_tables WHERE name ='trip_type')
    CREATE EXTERNAL TABLE bronze.trip_type(
         trip_type TINYINT,
        trip_type_desc VARCHAR(50)
    )WITH(
        LOCATION = 'raw/trip_type.tsv',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = tsv_file_format
    );

-- SELECT * FROM bronze.trip_type;

IF NOT EXISTS (SELECT * FROM sys.external_tables WHERE name = 'trip_data_csv')
    CREATE EXTERNAL TABLE bronze.trip_data_csv
    (
        VendorID INT,
        lpep_pickup_datetime datetime2(0),
        lpep_dropoff_datetime datetime2(0),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance DECIMAL(10,2),
        fare_amount DECIMAL(10,2),
        extra DECIMAL(10,2),
        mta_tax DECIMAL(10,2),
        tip_amount DECIMAL(10,2),
        tolls_amount DECIMAL(10,2),
        ehail_fee DECIMAL(10,2),
        improvement_surcharge DECIMAL(10,2),
        total_amount DECIMAL(10,2),
        payment_type INT,
        trip_type INT,
        congestion_surcharge  DECIMAL(8,2)
    )WITH(
        LOCATION = 'raw/trip_data_green_csv/**',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = csv_file_format
    );

-- SELECT * FROM bronze.trip_data_csv;

IF NOT EXISTS (SELECT * FROM sys.external_tables WHERE name = 'trip_data_parquet')
    CREATE EXTERNAL TABLE bronze.trip_data_parquet
    (
        VendorID INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee INT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge  FLOAT
    )WITH(
        LOCATION = 'raw/trip_data_green_parquet/**',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT = parquet_file_format
    );

-- SELECT top 100 * from bronze.trip_data_parquet;
IF NOT EXISTS (SELECT * FROM sys.external_tables WHERE name = 'trip_data_delta')
    CREATE EXTERNAL TABLE bronze.trip_data_delta
    (
        VendorID INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee INT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge  FLOAT
    )WITH(
        LOCATION = 'raw/trip_data_green_delta/',
        DATA_SOURCE = nyc_taxi_src,
        FILE_FORMAT  = delta_file_format
    );

SELECT top 100 * from bronze.trip_data_delta;



