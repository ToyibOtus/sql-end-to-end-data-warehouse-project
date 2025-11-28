/*
==========================================================================================================
Stored Procedure: Load Silver Layer(Bronze -> Silver)
==========================================================================================================
Script Purpose:
	This script performs incremental load on the silver table [silver.load_silver_archive_orders]. 
	It performs various transformations, such as data cleansing, data standardization, data enrichment,
	and deriving new columns, where necessary. In addition to this, it loads the etl log table 
	[audit.etl_log_table], enriching it with vital execution details.

Parameter: None

Usage: EXEC silver.load_silver_archive_orders;

Note:
	* Running this script retrieves run_id and source_batch_id from corresponding ingest tables.
	* Run the master procedure in the etl schema, as it performs a full ETL and  assigns a 
	  similar run_id across all tables, and layers. This allows for unified tracking, and 
	  thus enabling easy traceability and debugging.
==========================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver_archive_orders AS
BEGIN
	-- Ensure transaction auto-aborts on severe errors
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE 
	@run_id UNIQUEIDENTIFIER,
	@source_batch_id UNIQUEIDENTIFIER,
	@layer NVARCHAR(50) = 'silver',
	@table_loaded NVARCHAR(50) = 'archive_orders',
	@proc_name NVARCHAR(50) = 'load_silver_archive_orders',
	@load_type NVARCHAR(50) = 'Incremental',
	@ingest_source NVARCHAR(250) = 'bronze.archive_orders',
	@start_time DATETIME,
	@end_time DATETIME,
	@load_duration INT,
	@load_status NVARCHAR(50),
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT;

	BEGIN TRY
		-- Retrieve corresponding run_id from etl log table
		SELECT TOP 1 @run_id = etl_run_id FROM audit.etl_log_table  
		WHERE etl_layer = 'bronze' AND etl_table_loaded = 'archive_orders' 
		ORDER BY etl_start_time DESC;

		-- Throw an error if corresponding run_id is NULL
		IF @run_id IS NULL THROW 50001 , 'silver.archive_orders cannot load before bronze.archive_orders. Load aborted.', 1;

		-- Retrieve corresponding source_batch_id from etl log table
		SELECT TOP 1 @source_batch_id = etl_source_batch_id FROM audit.etl_log_table
		WHERE etl_layer = 'bronze' and etl_table_loaded = 'archive_orders'
		ORDER BY etl_start_time DESC;

		WITH new_records AS
		(
		SELECT bo.* FROM bronze.archive_orders bo
		LEFT JOIN silver.archive_orders so
		ON bo.dwh_row_hash = so.dwh_row_hash
		WHERE so.dwh_row_hash IS NULL
		)
		-- Retrieve total rows from corresponding bronze table
		SELECT @rows_source = COUNT(*) FROM new_records;

		-- Throw an error if total rows is NULL or zero
		IF @rows_source IS NULL OR @rows_source = 0 THROW 50002, 'No new records found in bronze.archive_orders. Load aborted.', 2;

		-- Capture load start time
		SET @start_time = GETDATE();

		-- Begin ETL transaction
		BEGIN TRAN;
		WITH new_records AS
		(
		SELECT bo.* FROM bronze.archive_orders bo
		LEFT JOIN silver.archive_orders so
		ON bo.dwh_row_hash = so.dwh_row_hash
		WHERE so.dwh_row_hash IS NULL
		)
		, cleaned_records AS
		(
		SELECT
			order_id,
			customer_id,
			order_time,
			CASE
				WHEN TRIM(payment_method) = 'cod' THEN 'cash_on_delivery'
				WHEN payment_method IS NULL THEN 'N/A'
				ELSE LOWER(TRIM(payment_method))
			END AS payment_method,
			discount_pct,
			subtotal_usd,
			total_usd,
			country,
			device,
			[source],
			dwh_batch_id,
			dwh_row_hash
		FROM new_records
		)
		INSERT INTO silver.archive_orders
		(
			order_id,
			customer_id,
			order_time,
			payment_method,
			discount_pct,
			subtotal_usd,
			total_usd,
			country,
			device,
			[source],
			dwh_batch_id,
			dwh_row_hash
		)
		SELECT
			order_id,
			customer_id,
			order_time,
			payment_method,
			discount_pct,
			subtotal_usd,
			total_usd,
			country,
			device,
			[source],
			dwh_batch_id,
			dwh_row_hash
		FROM cleaned_records;

		SET @rows_loaded = @@ROWCOUNT;

		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);
		SET @load_status = 'Success';
		SET @rows_diff = @rows_source - @rows_loaded;

		-- Load etl log table
		INSERT INTO audit.etl_log_table
		(
			etl_run_id,
			etl_source_batch_id,
			etl_layer,
			etl_load_type,
			etl_proc_name,
			etl_table_loaded,
			etl_ingest_source,
			etl_load_status,
			etl_start_time,
			etl_end_time,
			etl_load_duration_seconds,
			etl_rows_source,
			etl_rows_loaded,
			etl_rows_diff
		)
		VALUES
		(
			@run_id,
			@source_batch_id,
			@layer,
			@load_type,
			@proc_name,
			@table_loaded,
			@ingest_source,
			@load_status,
			@start_time,
			@end_time,
			@load_duration,
			@rows_source,
			@rows_loaded,
			@rows_diff
		);
	END TRY

	BEGIN CATCH
		-- Map a default value to batch_id if NULL
		IF @run_id IS NULL SET @run_id = '00000000-0000-0000-0000-000000000000';
		IF @source_batch_id IS NULL SET @source_batch_id = '00000000-0000-0000-0000-000000000000';

		-- Map a default value to start time if error occurs before ETL transaction
		IF @start_time IS NULL SET @start_time = GETDATE();

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);
		SET @load_status = 'Failed'

		-- Rollback any open transaction
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;
		
		-- Map value to rows source if NULL
		IF @rows_source IS NULL SET @rows_source = 0;
		-- Map value to rows loaded if NULL
		IF @rows_loaded IS NULL SET @rows_loaded = 0;
		-- Map value to row difference when error occurs 
		SET @rows_diff = @rows_source - @rows_loaded;

		-- Load log table with error details
		INSERT INTO audit.etl_log_table
		(
			etl_run_id,
			etl_source_batch_id,
			etl_layer,
			etl_table_loaded,
			etl_proc_name,
			etl_load_type,
			etl_ingest_source,
			etl_start_time,
			etl_end_time,
			etl_load_duration_seconds,
			etl_load_status,
			etl_rows_source,
			etl_rows_loaded,
			etl_rows_diff,
			etl_error_message,
			etl_error_number,
			etl_error_line,
			etl_error_severity,
			etl_error_state,
			etl_error_proc
		)
		VALUES
		(
			@run_id,
			@source_batch_id,
			@layer,
			@table_loaded,
			@proc_name,
			@load_type,
			@ingest_source,
			@start_time,
			@end_time,
			@load_duration,
			@load_status,
			@rows_source,
			@rows_loaded,
			@rows_diff,
			ERROR_MESSAGE(),
			ERROR_NUMBER(),
			ERROR_LINE(),
			ERROR_SEVERITY(),
			ERROR_STATE(),
			ERROR_PROCEDURE()
		);
	END CATCH;
END
GO
