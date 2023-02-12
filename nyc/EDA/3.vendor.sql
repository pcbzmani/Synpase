-- Databricks notebook source
use nyctaxi;

SELECT 
    *
    FROM
    OPENROWSET (
        BULK 'vendor_escaped.csv',
        DATA_SOURCE = 'fsnyctxi_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        ESCAPECHAR = '\\',
        HEADER_ROW = TRUE
    ) AS vendor;


SELECT 
    *
    FROM
    OPENROWSET (
        BULK 'vendor.csv',
        DATA_SOURCE = 'fsnyctxi_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDQUOTE = '"'
    ) AS vendor;
