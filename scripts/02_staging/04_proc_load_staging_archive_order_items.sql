/*
=========================================================================================================
Stored Procedure: Load Staging Layer (Source -> Staging)
=========================================================================================================
Script Purpose: 
	This script performs a full load on the staging table [staging.load_staging_archive_order_items]
	using the BULK INSERT command. In addition to this, it inserts vital log details into
	the etl log table [audit.etl_log_table], enabling easy etl monitoring, tracebaility, and
	debugging.

Parameter: None.

Usage: EXEC staging.load_staging_archive_order_items;

Note:
	* Running this script assigns a unique batch_id to this table.
	* Run the master procedure in the etl schema, as it performs a full ETL and 
	  assigns a similar batch_id across all layers, and tables. This allows for 
	  unified tracking, and thus enabling easy traceability and debugging.
=========================================================================================================
*/
CREATE OR ALTER PROCEDURE staging.load_staging_archive_order_items AS
BEGIN
	-- Declare variables
	DECLARE @batch_id UNIQUEIDENTIFIER = NEWID(),
	@layer NVARCHAR(50),
	@load_type NVARCHAR(50),
	@proc_name NVARCHAR(50),
	@table_loaded NVARCHAR(50),
	@ingest_source NVARCHAR(250),
	@load_status NVARCHAR(50),
	@start_time DATETIME,
	@end_time DATETIME,
	@load_duration INT,
	@rows_loaded INT,
	@sql NVARCHAR(MAX);

	BEGIN TRY
		-- Map values to variables
		SET @layer = 'staging';
		SET @load_type = 'Full';
		SET @proc_name = 'load_staging_archive_order_items';
		SET @table_loaded = 'archive_order_items';
		SET @ingest_source = 'C:\Users\PC\Desktop\archive (1)\order_items.csv';
		SET @start_time = GETDATE();

		-- Delete data from table
		SET @sql = 'TRUNCATE TABLE staging.archive_order_items';
		EXEC(@sql);

		-- Load into table
		SET @sql = 'BULK INSERT staging.archive_order_items FROM ''' + @ingest_source + '''' +
		'WITH(FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC(@sql);

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_status = 'Success';
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);
		SELECT @rows_loaded = COUNT(*) FROM staging.archive_order_items;

		-- Load into etl log table
		INSERT INTO audit.etl_log_table
		( 
			etl_batch_id,
			etl_layer, 
			etl_load_type, 
			etl_proc_name,
			etl_table_loaded,
			etl_ingest_source,
			etl_load_status,
			etl_start_time,
			etl_end_time,
			etl_load_duration_seconds,
			etl_rows_loaded
		)
		VALUES
		(
			@batch_id,
			@layer,
			@load_type,
			@proc_name,
			@table_loaded,
			@ingest_source,
			@load_status,
			@start_time,
			@end_time,
			@load_duration,
			@rows_loaded
		);
	END TRY

	BEGIN CATCH
		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);
		SET @load_status = 'Failed';

		-- Load into etl log table with error handling
		INSERT INTO audit.etl_log_table
		(
			etl_batch_id,
			etl_layer, 
			etl_load_type, 
			etl_proc_name,
			etl_table_loaded,
			etl_ingest_source,
			etl_load_status,
			etl_start_time,
			etl_end_time,
			etl_load_duration_seconds,
			etl_error_message,
			etl_error_number,
			etl_error_line,
			etl_error_severity,
			etl_error_state,
			etl_error_proc
		)
		VALUES
		(
			@batch_id,
			@layer,
			@load_type,
			@proc_name,
			@table_loaded,
			@ingest_source,
			@load_status,
			@start_time,
			@end_time,
			@load_duration,
			ERROR_MESSAGE(),
			ERROR_NUMBER(),
			ERROR_LINE(),
			ERROR_SEVERITY(),
			ERROR_STATE(),
			ERROR_PROCEDURE()
		);
	END CATCH;
END;
GO
