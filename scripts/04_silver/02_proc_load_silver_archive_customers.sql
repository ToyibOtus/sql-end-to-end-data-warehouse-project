/*
==========================================================================================================
Stored Procedure: Load Silver Layer(Bronze -> Silver)
==========================================================================================================
Script Purpose:
	This script performs incremental load on the silver table [silver.load_silver_archive_customers. 
	It performs various transformations, such as data cleansing, data standardization, data enrichment,
	and deriving new columns, where necessary. In addition to this, it loads the etl log table 
	[audit.etl_log_table], enriching it with vital execution details.

Parameter: None

Usage: EXEC silver.load_silver_archive_customers;

Note:
	* Running this script assigns a batch id from it corresponding bronze table.
	* Though all corresponding tables from each layer have similar batch_id, they vary across 
	  non-corresponding tables.
	* Run the master procedure in the etl schema, as it performs a full ETL and 
	  assigns a similar batch_id across all layers, and tables. This allows for 
	  unified tracking, and thus enabling easy traceability and debugging.
==========================================================================================================
*/
CREATE OR ALTER  PROCEDURE silver.load_silver_archive_customers AS
BEGIN
	-- Ensure transaction auto-aborts on severe errors
	SET XACT_ABORT ON;

	-- Declare and map values to variables where necessary
	DECLARE 
	@batch_id UNIQUEIDENTIFIER,
	@layer NVARCHAR(50) = 'silver',
	@table_loaded NVARCHAR(50) = 'archive_customers',
	@proc_name NVARCHAR(50) = 'load_silver_archive_customers',
	@load_type NVARCHAR(50) = 'Incremental',
	@ingest_source NVARCHAR(250) = 'bronze.archive_customers',
	@start_time DATETIME,
	@end_time DATETIME,
	@load_duration INT,
	@load_status NVARCHAR(50),
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT;

	BEGIN TRY
		-- Retrieve corresponding batch_id from etl log table
		SELECT TOP 1 @batch_id = etl_batch_id FROM audit.etl_log_table  WHERE etl_layer = 'bronze' 
		AND etl_table_loaded = 'archive_customers' ORDER BY etl_start_time DESC;

		-- Throw an error if corresponding batch_id is NULL
		IF @batch_id IS NULL THROW 50001 , 'silver.archive_customers cannot load before bronze.archive_customers. Load aborted.', 1;

		WITH new_records AS
		(
		SELECT bc.* FROM bronze.archive_customers bc
		LEFT JOIN silver.archive_customers sc
		ON bc.dwh_row_hash = sc.dwh_row_hash
		WHERE sc.dwh_row_hash IS NULL
		)
		-- Retrieve total rows from corresponding bronze table
		SELECT @rows_source = COUNT(*) FROM new_records;

		-- Throw an error if total rows is NULL or zero
		IF @rows_source IS NULL OR @rows_source = 0 THROW 50002, 'No new records found in bronze.archive_customers. Load aborted.', 2;

		-- Capture load start time
		SET @start_time = GETDATE();

		-- Begin ETL transaction
		BEGIN TRAN;
		WITH new_records AS
		(
		SELECT bc.* FROM bronze.archive_customers bc
		LEFT JOIN silver.archive_customers sc
		ON bc.dwh_row_hash = sc.dwh_row_hash
		WHERE sc.dwh_row_hash IS NULL
		)
		, cleaned_records AS
		(
		SELECT 
			customer_id,
			[name] AS [name],
			email AS email,
			country,
			CASE TRIM(country)
				WHEN 'DE' THEN 'germany'
				WHEN 'IN' THEN 'india'
				WHEN 'GB' THEN 'united_kingdom'
				WHEN 'SE' THEN 'sweden'
				WHEN 'NL' THEN 'netherlands'
				WHEN 'AU' THEN 'australia'
				WHEN 'MX' THEN 'mexico'
				WHEN 'CA' THEN 'canada'
				WHEN 'BR' THEN 'brazil'
				WHEN 'ZA' THEN 'south_africa'
				WHEN 'PL' THEN 'poland'
				WHEN 'FR' THEN 'france'
				WHEN 'US' THEN 'united_states'
				WHEN 'AE' THEN 'united_arab_emirates'
				WHEN 'JP' THEN 'japan'
				WHEN 'ES' THEN 'spain'
				WHEN 'SG' THEN 'singapore'
				ELSE 'N/A'
			END AS country_name,
			age,
			signup_date,
			marketing_opt_in AS marketing_opt_in,
			dwh_batch_id,
			dwh_row_hash
		FROM new_records
		)
		INSERT INTO silver.archive_customers
		(
			customer_id,
			[name],
			email,
			country,
			country_name,
			age,
			signup_date,
			marketing_opt_in,
			dwh_batch_id,
			dwh_row_hash
		)
		SELECT
			customer_id,
			[name],
			email,
			country,
			country_name,
			age,
			signup_date,
			marketing_opt_in,
			dwh_batch_id,
			dwh_row_hash
		FROM cleaned_records;

		-- Retrieve rows loaded at the end of the transaction
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
	END CATCH
END
GO
