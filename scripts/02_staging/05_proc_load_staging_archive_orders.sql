/*
=========================================================================================================
Stored Procedure: Load Staging Layer (Source -> Staging)
=========================================================================================================
Script Purpose: 
	This script performs a full load on the staging table [staging.load_staging_archive_orders]
	using the BULK INSERT command. In addition to this, it inserts vital log details into
	the etl log table [audit.etl_log_table], enabling easy etl monitoring, tracebaility, and
	debugging.

Parameter: @batch_id UNIQUEIDENTIFIER

Usage: EXEC staging.load_staging_archive_orders;

Note:
	* Running this script assigns a unique batch_id to this table.
	* Run the master procedure in the etl schema, as it performs a full ETL and 
	  assigns a similar batch_id across all layers, and tables. This allows for 
	  unified tracking, and thus enabling easy traceability and debugging.
=========================================================================================================
*/
CREATE OR ALTER PROCEDURE staging.load_staging_archive_orders @batch_id UNIQUEIDENTIFIER = NULL AS
BEGIN
	-- Ensure transaction auto-aborts on severe errors
	SET XACT_ABORT ON;
	
	-- Declare & map values to variables where necessary
	DECLARE 
	@layer NVARCHAR(50) = 'staging',
	@table_loaded NVARCHAR(50) = 'archive_orders',
	@proc_name NVARCHAR(50) = 'load_staging_archive_orders',
	@load_type NVARCHAR(50) = 'Full',
	@ingest_source NVARCHAR(250) = 'C:\Users\PC\Desktop\archive (1)\orders.csv',
	@start_time DATETIME,
	@end_time DATETIME,
	@load_duration INT,
	@load_status NVARCHAR(50),
	@rows_loaded INT,
	@rows_diff INT,
	@sql NVARCHAR(MAX);

	-- Generate batch_id if not supplied
	IF @batch_id IS NULL SET @batch_id = NEWID();

	BEGIN TRY
		-- Capture load start time
		SET @start_time = GETDATE();

		-- Start ETL transaction
		BEGIN TRAN;
		-- Delete data from table
		TRUNCATE TABLE staging.archive_orders;

		-- Load into table
		SET @sql = 'BULK INSERT staging.archive_orders FROM ''' + @ingest_source + '''' +
		'WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC(@sql);

		SET @rows_loaded = @@ROWCOUNT;
		IF @rows_loaded IS NULL OR @rows_loaded = 0 THROW 50002, 'No rows loaded from file.', 2;

		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_status = 'Success';
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);

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
		-- If load failed early, assign a start time
		IF @start_time IS NULL SET @start_time = GETDATE();

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);
		SET @load_status = 'Failed'

		-- Rollback any open transactions
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;

		-- Ensure rows loaded is not null
		IF @rows_loaded IS NULL SET @rows_loaded = 0;

		INSERT INTO audit.etl_log_table
		(
		etl_batch_id,
		etl_layer,
		etl_table_loaded,
		etl_proc_name,
		etl_load_type,
		etl_ingest_source,
		etl_start_time,
		etl_end_time,
		etl_load_duration_seconds,
		etl_load_status,
		etl_rows_loaded,
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
		@table_loaded,
		@proc_name,
		@load_type,
		@ingest_source,
		@start_time,
		@end_time,
		@load_duration,
		@load_status,
		@rows_loaded,
		ERROR_MESSAGE(),
		ERROR_NUMBER(),
		ERROR_LINE(),
		ERROR_SEVERITY(),
		ERROR_STATE(),
		ERROR_PROCEDURE()
		)
	END CATCH;
END;
GO
