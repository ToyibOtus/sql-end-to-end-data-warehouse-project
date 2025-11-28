/*
========================================================================================================
Stored Procedure: Load Bronze Layer (Staging -> Bronze)
========================================================================================================
Script Purpose:
	This script loads data directly from staging table [staging.archive_order_items] into its
	corresponding bronze table. In addition to this, it adds vital metadata information, preparing
	it not only for easy tracebility and debugging, but for subsequent incremental load of the
	silver layer. It also adds vital etl log information to the elt log table.

Parameter: None.

Usage: EXEC bronze.load_bronze_archive_order_items;

Note:
	* Running this retrieves run_id and source_batch_id from corresponding ingest tables.
	* Run the master procedure in the etl schema, as it performs a full ETL and  assigns a 
	  similar run_id across all tables, and layers. This allows for unified tracking, and 
	  thus enabling easy traceability and debugging.
========================================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze_archive_order_items AS 
BEGIN
	-- Ensure transaction auto-aborts on severe errors
	SET XACT_ABORT ON;
	
	-- Declare and map values to variables where necessary
	DECLARE 
	@run_id UNIQUEIDENTIFIER,
	@source_batch_id UNIQUEIDENTIFIER,
	@layer NVARCHAR(50) = 'bronze',
	@table_loaded NVARCHAR(50) = 'archive_order_items',
	@proc_name NVARCHAR(50) = 'load_bronze_archive_order_items',
	@load_type NVARCHAR(50) = 'Full',
	@ingest_source NVARCHAR(250) = 'staging.archive_order_items',
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
		WHERE etl_layer = 'staging' AND etl_table_loaded = 'archive_order_items' 
		ORDER BY etl_start_time DESC;

		-- Throw an error if corresponding run_id is NULL
		IF @run_id IS NULL THROW 50001, 
		'bronze.archive_order_items cannot load before staging.archive_order_items. Load aborted.', 1;

		-- Retrieve corresponding source_batch_id from etl log table
		SELECT TOP 1 @source_batch_id = etl_source_batch_id FROM audit.etl_log_table
		WHERE etl_layer = 'staging' and etl_table_loaded = 'archive_order_items'
		ORDER BY etl_start_time DESC;

		-- Retrieve total rows from corresponding staging table
		SELECT @rows_source = COUNT(*) FROM staging.archive_order_items;

		-- Throw an error if total rows is NULL or zero
		IF @rows_source IS NULL OR @rows_source = 0 THROW 50002, 'No records found in staging.archive_order_items. Load aborted', 2

		-- Capture load start time
		SET @start_time = GETDATE();

		-- Begin ETL transaction
		BEGIN TRAN
		
		-- Delete data from table
		TRUNCATE TABLE bronze.archive_order_items;

		-- Load data into table
		INSERT INTO bronze.archive_order_items
		(
			order_id,
			product_id,
			unit_price_usd,
			quantity,
			line_total_usd,
			dwh_batch_id,
			dwh_raw_rows,
			dwh_row_hash
		)
		SELECT
			order_id,
			product_id,
			unit_price_usd,
			quantity,
			line_total_usd,
			@source_batch_id AS dwh_batch_id,
			CONCAT_WS('|', UPPER(CAST(order_id AS VARCHAR(50))), UPPER(CAST(product_id AS VARCHAR(50))), 
			UPPER(CAST(unit_price_usd AS VARCHAR(50))), UPPER(CAST(quantity AS VARCHAR(50))), 
			UPPER(CAST(line_total_usd AS VARCHAR(50)))) AS dwh_raw_rows,
			HASHBYTES('SHA2_256', CONCAT_WS('|', UPPER(CAST(order_id AS VARCHAR(50))) COLLATE LATIN1_GENERAL_100_BIN2, 
			UPPER(CAST(product_id AS VARCHAR(50))) COLLATE LATIN1_GENERAL_100_BIN2, UPPER(CAST(unit_price_usd AS VARCHAR(50))) 
			COLLATE LATIN1_GENERAL_100_BIN2, UPPER(CAST(quantity AS VARCHAR(50))) COLLATE LATIN1_GENERAL_100_BIN2, 
			UPPER(CAST(line_total_usd AS VARCHAR(50))) COLLATE LATIN1_GENERAL_100_BIN2)) AS dwh_row_hash
		FROM staging.archive_order_items;

		-- Finalize transaction on success
		COMMIT TRAN;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_status = 'Success';
		SELECT @rows_loaded = COUNT(*) FROM bronze.archive_order_items;
		SET @rows_diff = @rows_source - @rows_loaded;
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);

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
		-- Map a default value to run & batch id if NULL
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
		)
	END CATCH;
END;
GO
