CREATE OR ALTER   PROCEDURE silver.load_silver_archive_sessions AS
BEGIN
	-- Ensure transaction auto-aborts on severe errors
	SET XACT_ABORT ON;
	
	-- Declare and map values to variables where necessary
	DECLARE 
	@batch_id UNIQUEIDENTIFIER,
	@layer NVARCHAR(50) = 'silver',
	@table_loaded NVARCHAR(50) = 'archive_sessions',
	@proc_name NVARCHAR(50) = 'load_silver_archive_sessions',
	@load_type NVARCHAR(50) = 'Incremental',
	@ingest_source NVARCHAR(250) = 'bronze.archive_sessions',
	@start_time DATETIME,
	@end_time DATETIME,
	@load_duration INT,
	@load_status NVARCHAR(50),
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT;

	BEGIN TRY
		-- Retrieve corresponding batch_id from etl log table
		SELECT TOP 1 @batch_id = etl_batch_id FROM audit.etl_log_table WHERE
		etl_layer = 'bronze' AND etl_table_loaded = 'archive_sessions'
		ORDER BY etl_start_time DESC;

		-- Throw an error if corresponding batch_id is NULL
		IF @batch_id IS NULL THROW 50001, 'silver.archive_sessions cannot load before bronze.archive_sessions. Load aborted.', 1;

		WITH new_records AS
		(
		SELECT bs.* FROM bronze.archive_sessions bs
		LEFT JOIN silver.archive_sessions ss
		ON bs.dwh_row_hash = ss.dwh_row_hash
		WHERE ss.dwh_row_hash IS NULL
		)
		-- Retrieve total rows from corresponding bronze table
		SELECT @rows_source = COUNT(*) FROM new_records;

		-- Throw an error if total rows is NULL or zero
		IF @rows_source IS NULL OR @rows_source = 0 THROW 50002, 'No new records found in bronze.archive_sessions. Load aborted.', 2;

		-- Capture load start time
		SET @start_time = GETDATE();

		-- Begin ETL transaction
		BEGIN TRAN;
		WITH new_records AS
		(
		SELECT bs.* FROM bronze.archive_sessions bs
		LEFT JOIN silver.archive_sessions ss
		ON bs.dwh_row_hash = ss.dwh_row_hash
		WHERE ss.dwh_row_hash IS NULL
		)
		INSERT INTO silver.archive_sessions
		(
			[session_id],
			customer_id,
			start_time,
			device,
			[source],
			country,
			dwh_batch_id,
			dwh_row_hash
		)
		SELECT
			[session_id],
			customer_id,
			start_time,
			device,
			[source],
			country,
			dwh_batch_id,
			dwh_row_hash
		FROM bronze.archive_sessions;

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
			etl_rows_source,
			etl_rows_loaded,
			etl_rows_diff
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
			@rows_source,
			@rows_loaded,
			@rows_diff
		);
	END TRY

	BEGIN CATCH
		-- Map a default value to batch_id if NULL
		IF @batch_id IS NULL SET @batch_id = '00000000-0000-0000-0000-000000000000';

		-- Map a default value to start time if error occurs before ETL transaction
		IF @start_time IS NULL SET @start_time = GETDATE();

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);
		SET @load_status = 'Failed'

		-- Rollback any open transaction
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;

		-- Map value to rows loaded if NULL
		IF @rows_loaded IS NULL SET @rows_loaded = 0;

		-- Map value to row difference when an error occurs
		SET @rows_diff = @rows_source - @rows_loaded;

		-- Load etl log table with error details
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
END
GO
