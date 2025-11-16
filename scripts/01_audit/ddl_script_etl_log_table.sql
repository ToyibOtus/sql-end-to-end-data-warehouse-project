/*
=======================================================================
DDL Script: Create an ETL Log Table
=======================================================================
Script Purpose: 
	This script creates an etl log table. 
	
	Run this script to redefine the structure of your etl log table.
=======================================================================
*/
-- Drop Table [audit.etl_log_table] if it exists
IF OBJECT_ID('audit.etl_log_table', 'U') IS NOT NULL
DROP TABLE audit.etl_log_table;
GO

-- Create Table [audit.etl_log_table]
CREATE TABLE audit.etl_log_table
(
	etl_log_id INT NOT NULL IDENTITY(1, 1) PRIMARY KEY,
	etl_batch_id UNIQUEIDENTIFIER NOT NULL,
	etl_layer NVARCHAR(50) NOT NULL,
	etl_load_type NVARCHAR(50) NOT NULL,
	etl_proc_name NVARCHAR(50) NOT NULL,
	etl_table_loaded NVARCHAR(50) NOT NULL,
	etl_ingest_source NVARCHAR(250) NOT NULL,
	etl_load_status NVARCHAR(50) NOT NULL,
	etl_start_time DATETIME NOT NULL,
	etl_end_time DATETIME NOT NULL,
	etl_load_duration_seconds INT NOT NULL,
	etl_rows_source INT,
	etl_rows_loaded INT,
	etl_rows_diff INT,
	etl_error_message NVARCHAR(MAX),
	etl_error_number INT,
	etl_error_line INT,
	etl_error_severity INT,
	etl_error_state INT,
	etl_error_proc NVARCHAR(50)
);
