-- Databricks notebook source
IF (NOT EXISTS(SELECT * FROM sys.credentials WHERE name = 'synapse-project'))
    CREATE CREDENTIAL [synapse-project]
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = '1mPPL0rgadZcWuIrn18RzfzyWRcbVJBUiMpsjwlnzSLoSkkcwmuDEaKmNAZdt47Lk6WGK0ZRxwwvACDbe6UNoQ=='
GO

SELECT TOP 100 *
FROM OPENROWSET(​PROVIDER = 'CosmosDB',
                CONNECTION = 'Account=synapse-project;Database=nyctxidb',
                OBJECT = 'heartbeat',
                SERVER_CREDENTIAL = 'synapse-project'
) AS [heartbeat]

