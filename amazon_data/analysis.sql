-- Databricks notebook source
use amz_data;

ALTER DATABASE nyc_taxi_ldw collate Latin1_General_100_BIN2_UTF8
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name= 'amz_ds')
    CREATE EXTERNAL DATA SOURCE amz_ds
    WITH (
        LOCATION ='https://dsamazondata.dfs.core.windows.net/ordersdata'
    );

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'csv_file_format')
    CREATE EXTERNAL FILE FORMAT csv_file_format
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS (
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2,
            USE_TYPE_DEFAULT = FALSE,
            ENCODING = 'UTF8',
            PARSER_VERSION  = '2.0'
        )
    )
GO


USE amz_data
GO


IF object_id('silver.orders_data') is not NULL 
    DROP EXTERNAL TABLE silver.orders_data
GO

CREATE EXTERNAL TABLE silver.orders_data
WITH(
    LOCATION = 'silver/orders',
    DATA_SOURCE = amz_ds,
    FILE_FORMAT = csv_file_format
)
AS
SELECT 
    [Website] as website,
    [Order ID] as order_id,
    [Order Date] as order_dt,
    [Currency] as currency,
    [Unit Price] as unit_price,
    [Unit Price Tax] as unit_price_tax,
    [Shipping Charge] as shipping_charge,
    [Total Discounts] as tot_discount,
    [Total Owed] as tot_owed,
    [Shipment Item Subtotal] as shipment_item_subtotal,
    [Shipment Item Subtotal Tax] as shipment_item_subtotal_tax,
    [Quantity] as qty,
    [Order Status] as ordr_status,
    [Ship Date] as ship_dt,
    [Product Name] as prodt_name 
FROM bronze.orders_data
GO

SELECT * from silver.orders_data;





-- COMMAND ----------

USE amz_data
GO

-- IF OBJECT_ID('bronze.orders_data') is not NULL  
--     drop EXTERNAL TABLE bronze.orders_data

IF NOT EXISTS (SELECT * FROM sys.external_tables WHERE name = 'orders_data')
    CREATE EXTERNAL TABLE bronze.orders_data
    (
       [Website]  varchar(50),
        [Order ID]  varchar(50),
        [Order Date] varchar(50),
        [Purchase Order Number] varchar(10),
        [Currency] varchar(10),
        [Unit Price] varchar(15),
        [Unit Price Tax] varchar(15),
        [Shipping Charge] varchar(15),
        [Total Discounts] varchar(15),
        [Total Owed] varchar(15),
        [Shipment Item Subtotal] varchar(15),
        [Shipment Item Subtotal Tax] varchar(15),
        [ASIN] varchar(50),
        [Product Condition] varchar(25),
        [Quantity] INT,
        [Payment Instrument Type] varchar(100),
        [Order Status] varchar(25),
        [Shipment Status] varchar(25),
        [Ship Date]  varchar(50),
        [Shipping Option] varchar(100),
        [Shipping Address] varchar(350),
        [Billing Address] varchar(350),
        [Carrier Name & Tracking Number] varchar(350),
        [Product Name] varchar(350),
        [Gift Message] varchar(350),
        [Gift Sender Name] varchar(350),
        [Gift Recipient Contact Details] varchar(350)
    )
    WITH (
        LOCATION = 'orders_data/Retail.OrderHistory.2.csv',
        DATA_SOURCE = amz_ds,
        FILE_FORMAT = csv_file_format,
        REJECT_VALUE = 10,
        REJECTED_ROW_LOCATION = 'rejection/amzaon_data'
    )
    GO

SELECT * from bronze.orders_data

-- COMMAND ----------



CREATE VIEW gold.spend_habbit AS
SELECT 
    CONCAT(SUBSTRING(order_dt, 7, 4),
    SUBSTRING(order_dt, 1, 2)) as yearmnth,  
    sum(CAST(REPLACE(tot_owed, ',', '') as FLOAT)) as total_spend
FROM silver.orders_data
    WHERE ordr_status = 'Closed'
    GROUP BY  CONCAT(SUBSTRING(order_dt, 7, 4),
    SUBSTRING(order_dt, 1, 2));


SELECT * from  gold.spend_habbit
ORDER  by total_spend
