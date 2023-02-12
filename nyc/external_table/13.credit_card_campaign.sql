-- Databricks notebook source
USE nyc_taxi_ldw
GO

SELECT 
    td.year,
    td.month,
    CAST(td.lpep_pickup_datetime as date) as date_of_trip,
    cal.day_name,
    tz.borough,
    SUM(CASE
        WHEN pt.payment_desc_value = 'Credit card' THEN 1
        ELSE 0
        END) as credit_card_trans,
    SUM(CASE
        WHEN pt.payment_desc_value = 'Cash' THEN 1
        ELSE 0
        END) as cash_trans, 
    CASE 
        WHEN cal.day_name IN ('Saturday','Sunday') THEN 'Y' 
        ELSE 'N'
        END AS weekend_ind
    FROM silver.vw_green_trip_data td
    JOIN silver.calendar cal 
    ON CAST(td.lpep_pickup_datetime AS date) = cal.date
    JOIN silver.taxi_zone tz
    ON tz.location_id = td.PULocationID
    JOIN silver.payment_type pt 
    ON td.payment_type = pt.payment_type
WHERE td.year = '2021'
and td.month = '01'
    GROUP BY 
    td.year,
    td.month,
    CAST(td.lpep_pickup_datetime as date),
    cal.day_name,
    tz.borough;

EXEC gold.credit_card_campaign '2020','01';
EXEC gold.credit_card_campaign '2020','02';
EXEC gold.credit_card_campaign '2020','03';
EXEC gold.credit_card_campaign '2020','04';
EXEC gold.credit_card_campaign '2020','05';
EXEC gold.credit_card_campaign '2020','06';
EXEC gold.credit_card_campaign '2020','07';
EXEC gold.credit_card_campaign '2020','08';
EXEC gold.credit_card_campaign '2020','09';
EXEC gold.credit_card_campaign '2020','10';
EXEC gold.credit_card_campaign '2020','11';
EXEC gold.credit_card_campaign '2020','12';
EXEC gold.credit_card_campaign '2021','01';
EXEC gold.credit_card_campaign '2021','02';
EXEC gold.credit_card_campaign '2021','03';
EXEC gold.credit_card_campaign '2021','04';
EXEC gold.credit_card_campaign '2021','05';
EXEC gold.credit_card_campaign '2021','06';
