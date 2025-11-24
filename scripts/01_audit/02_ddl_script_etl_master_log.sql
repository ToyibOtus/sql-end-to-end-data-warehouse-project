/*
=======================================================================
DDL Script: Create an ETL Master Log
=======================================================================
Script Purpose: 
	This script creates an etl master log. 
	
	Run this script to redefine the structure of your master log table.
=======================================================================
*/
IF OBJECT_ID('audit.master_etl_log', 'U') IS NOT NULL
DROP TABLE audit.master_etl_log;
GO

CREATE TABLE audit.master_etl_log
(
	etl_log_id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
	etl_run_id UNIQUEIDENTIFIER NOT NULL,
	etl_pipeline_name NVARCHAR(50) NOT NULL,
	etl_start_time DATETIME NOT NULL,
	etl_end_time DATETIME NOT NULL,
	etl_load_duration_seconds INT NOT NULL,
	etl_load_status NVARCHAR(50) NOT NULL,
	etl_error_number INT,
	etl_error_message NVARCHAR(MAX),
	etl_error_state INT,
	etl_error_line INT,
	etl_error_severity INT,
	etl_error_procedure NVARCHAR(50),
	etl_create_time DATETIME DEFAULT GETDATE()
);
