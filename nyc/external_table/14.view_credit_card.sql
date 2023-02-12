-- Databricks notebook source
USE nyc_taxi_ldw
GO

DROP VIEW IF EXISTS gold.vw_credit_card_campaign
GO

CREATE VIEW gold.vw_credit_card_campaign
AS
SELECT
    creditcard.filepath(1) as year,
    creditcard.filepath(2) as month,
    creditcard.*
FROM
    OPENROWSET(
        BULK 'gold/trip_data_green/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'PARQUET'
    )WITH(
        borough VARCHAR(15),
        date_of_trip date,
        day_name VARCHAR(10),
        weekend_ind CHAR(1),
        credit_card_trans INT,
        cash_trans INT,
        dispatch_count INT,
        street_hail_count INT,
        trip_duration INT,
        trip_distance FLOAT,
        fare_amount FLOAT
    ) AS creditcard
GO

SELECT * from gold.vw_credit_card_campaign
WHERE year = '2020' and month = '01';
