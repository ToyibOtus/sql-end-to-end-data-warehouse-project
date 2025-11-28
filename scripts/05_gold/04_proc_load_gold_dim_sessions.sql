/*
==========================================================================================================
Stored Procedure: Load Gold Layer(Silver -> Gold)
==========================================================================================================
Script Purpose:
	This script performs incremental load on the gold table [gold.dim_sessions], and 
	categorizes the it into fact or dimension based on the content of data held in the ingest source.
	Additionally, it performs data integration and data modeling to support easy analysis. It also
	enriches the log table [audit.etl_log_table] with vital execution details.
	
Parameter: None

Usage: EXEC gold.load_gold_dim_sessions;

Note:
	* Running this script retrieves run_id and source_batch_id from corresponding ingest tables.
	* Run the master procedure in the etl schema, as it performs a full ETL and  assigns a 
	  similar run_id across all tables, and layers. This allows for unified tracking, and 
	  thus enabling easy traceability and debugging.
==========================================================================================================
*/
CREATE OR ALTER PROCEDURE gold.load_gold_dim_sessions AS
BEGIN
	-- Ensure transaction auto-aborts on severe errors
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE
	@run_id UNIQUEIDENTIFIER,
	@source_batch_id UNIQUEIDENTIFIER,
	@layer NVARCHAR(50) = 'gold',
	@table_loaded NVARCHAR(50) = 'dim_sessions',
	@proc_name NVARCHAR(50) = 'load_gold_dim_sessions',
	@load_type NVARCHAR(50) = 'Incremental',
	@ingest_source NVARCHAR(50) = 'silver.archive_sessions',
	@start_time DATETIME,
	@end_time DATETIME,
	@load_duration INT,
	@load_status NVARCHAR(50),
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT;

	BEGIN TRY
		-- Retrieve corresponding run_id from etl_log_table
		SELECT TOP 1 @run_id = etl_run_id FROM audit.etl_log_table
		WHERE etl_layer = 'silver' and etl_table_loaded = 'archive_sessions'
		ORDER BY etl_start_time DESC;

		-- Throw an error if run_id is null
		IF @run_id IS NULL THROW 50001, 'gold.dim_sessions cannot load before silver.archive_sessions. Load aborted.', 1;

		-- Retrieve corresponding source_batch_id from etl_log_table
		SELECT TOP 1 @source_batch_id = etl_source_batch_id FROM audit.etl_log_table
		WHERE etl_layer = 'silver' and etl_table_loaded = 'archive_sessions'
		ORDER BY etl_start_time DESC;

		WITH new_records AS
		(
		SELECT sas.* FROM silver.archive_sessions sas
		LEFT JOIN gold.dim_sessions ds
		ON sas.dwh_row_hash = ds.dwh_row_hash
		WHERE ds.dwh_row_hash IS NULL
		)
		-- Retrieve total number of rows from ingest table
		SELECT @rows_source = COUNT(*) FROM new_records;

		-- Throw an error if total number of rows is null or zero
		IF @rows_source IS NULL OR @rows_source = 0 THROW 50002, 'No new records found in silver.archive_sessions. Load aborted.', 2;

		-- Capture start time
		SET @start_time = GETDATE();

		-- Begin ETL transaction
		BEGIN TRAN;
		WITH new_records AS
		(
		SELECT sas.* FROM silver.archive_sessions sas
		LEFT JOIN gold.dim_sessions ds
		ON sas.dwh_row_hash = ds.dwh_row_hash
		WHERE ds.dwh_row_hash IS NULL
		)
		, integrated_data AS
		(
		SELECT
			nr.[session_id] AS user_session_id,
			c.customer_key,
			nr.start_time,
			nr.device,
			nr.[source] AS traffic_source,
			nr.country,
			nr.dwh_batch_id,
			nr.dwh_row_hash,
			nr.dwh_create_time
		FROM new_records nr
		LEFT JOIN gold.dim_customers c
		ON nr.customer_id = c.customer_id
		)
		INSERT INTO gold.dim_sessions
		(
			user_session_id,
			customer_key,
			start_time,
			device,
			traffic_source,
			country,
			dwh_batch_id,
			dwh_row_hash,
			dwh_create_time
		)
		SELECT
			user_session_id,
			customer_key,
			start_time,
			device,
			traffic_source,
			country,
			dwh_batch_id,
			dwh_row_hash,
			dwh_create_time
		FROM integrated_data;

		-- Retrieve rows loaded
		SET @rows_loaded = @@ROWCOUNT;

		-- Finalize etl transaction on success
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
			etl_rows_diff
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

		-- Load etl log table with error details
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
END;
GO
