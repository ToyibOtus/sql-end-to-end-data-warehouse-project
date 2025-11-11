/*
==========================================================================================
Create Database &Schemas
==========================================================================================
Script Purpose:
	This script creates a new database [Datawarehouse] from the scratch. It firstly 
	checks the existence of this database, drops it if it exists, and recreates it from
	the scratch. In addition, it creates 5 schemas:
	* bronze,
	* silver,
	* gold,
	* audit, and
	* etl.

Warning: 
	Running this script permanently deletes the [Datawarehouse] database, and all 
	data inside it. 
	Ensure to have proper back ups before running.
==========================================================================================
*/
USE master;
GO
-- Drop database [Datawarehouse] if it exists
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE
DROP DATABASE Datawarehouse;
GO
-- Creata database [Datawarehouse]
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO
-- Create schema [bronze] if it doesn't exist
IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'bronze')
EXEC ('CREATE SCHEMA bronze');
GO
-- Create schema [silver] if it doesn't exist
IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'silver')
EXEC ('CREATE SCHEMA silver');
GO
-- Create schema [gold] if it doesn't exist
IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'gold')
EXEC ('CREATE SCHEMA gold');
GO
-- Create schema [audit] if it doesn't exist
IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'audit')
EXEC ('CREATE SCHEMA audit');
GO
-- Create schema [etl] if it doesn't exist
IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'etl')
EXEC ('CREATE SCHEMA etl');
GO
