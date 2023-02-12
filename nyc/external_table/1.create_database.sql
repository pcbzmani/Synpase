-- Databricks notebook source
use master
GO

CREATE DATABASE nyc_taxi_ldw
GO

ALTER DATABASE nyc_taxi_ldw collate Latin1_General_100_BIN2_UTF8
GO

use nyc_taxi_ldw
GO

CREATE SCHEMA bronze
GO

CREATE SCHEMA silver
GO

CREATE SCHEMA gold
GO
