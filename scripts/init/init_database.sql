/*
===============================================================================
Database and Schema Setup
===============================================================================
Script Purpose:
    This script creates a new database named 'OlistDW' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'OlistDW' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.

*/

USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'OlistDW')
BEGIN
    ALTER DATABASE OlistDW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE OlistDW;
END;
GO

CREATE DATABASE OlistDW;
GO

USE OlistDW;
GO


CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO