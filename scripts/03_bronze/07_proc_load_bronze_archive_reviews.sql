/*
========================================================================================================
Stored Procedure: Load Bronze Layer (Staging -> Bronze)
========================================================================================================
Script Purpose:
	This script loads data directly from staging table [staging.archive_reviews] into its
	corresponding bronze table. It also performs light transformations, specifically 
	[Field Repair & Column Combination]. In addition to this, it adds vital metadata information, 
	preparing it not only for easy tracebility and debugging, but for subsequent incremental load 
	of the silver layer. It also adds vital etl log information to the elt log table.

Parameter: None.

Usage: EXEC bronze.load_bronze_archive_reviews;

Note:
	* Running this script assigns a unique batch_id to this table.
	* Run the master procedure in the etl schema, as it performs a full ETL and 
	  assigns a similar batch_id across all layers, and tables. This allows for 
	  unified tracking, and thus enabling easy traceability and debugging.
========================================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze_archive_reviews AS
BEGIN
	-- Declare Variables
	DECLARE
	@batch_id UNIQUEIDENTIFIER = NEWID(),
	@ingest_source NVARCHAR(250),
	@layer NVARCHAR(50),
	@load_type NVARCHAR(50),
	@proc_name NVARCHAR(50),
	@table_loaded NVARCHAR(50),
	@load_status NVARCHAR(50),
	@start_time DATETIME,
	@end_time DATETIME,
	@load_duration INT,
	@rows_source INT,
	@rows_loaded INT,
	@rows_diff INT;

	BEGIN TRY
		-- Map values to variables
		SET @layer = 'bronze';
		SET @load_type = 'Full';
		SET @proc_name = 'load_bronze_archive_reviews';
		SELECT @rows_source = COUNT(*) FROM staging.archive_reviews;
		SET @ingest_source = 'staging.archive_reviews'
		SET @start_time = GETDATE();

		-- Delete data from table
		TRUNCATE TABLE bronze.archive_reviews

		-- Load data into table
		INSERT INTO bronze.archive_reviews
		(
		review_id,
		order_id,
		product_id,
		rating,
		review_text,
		review_time,
		dwh_batch_id,
		dwh_raw_rows,
		dwh_row_hash
		)
		SELECT
		review_id,
		order_id,
		product_id,
		rating,
		review_text,
		review_time,
		dwh_batch_id,
		CONCAT_WS('|', review_id, order_id, product_id, rating, review_text, review_time) AS dwh_raw_rows,
		HASHBYTES('SHA2_256', CONCAT_WS('|', review_id, order_id, product_id, 
		rating, review_text, review_time)) AS dwh_row_hash
		FROM
		(
		SELECT
		review_id,
		order_id,
		product_id,
		rating,
		CASE 
			WHEN review_text_sub_str IS NULL THEN review_text
			ELSE CONCAT(review_text, ', ', review_text_sub_str)
		END AS review_text,
		review_time,
		dwh_batch_id
		FROM
		(
		SELECT
			review_id,
			order_id,
			product_id,
			rating, 
			review_text,
			CASE
				WHEN LEN(TRIM(review_time)) = 19 THEN review_time
				ELSE RIGHT(TRIM(review_time), 19) 
			END AS review_time,
			CASE 
				WHEN LEN(TRIM(review_time) ) > 19 THEN 
				SUBSTRING(TRIM(review_time), 0 , LEN(review_time) - LEN(RIGHT(TRIM(review_time), 19)))
				ELSE NULL
			END AS review_text_sub_str,
			@batch_id AS dwh_batch_id
		FROM staging.archive_reviews
		)SUB1
		)SUB2;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @table_loaded = 'archive_reviews';
		SET @load_status = 'Success';
		SELECT @rows_loaded = COUNT(*) FROM bronze.archive_reviews;
		SET @rows_diff = @rows_source - @rows_loaded;
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);

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
		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_status = 'Failed';
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);

		-- Load etl log table with vital error details
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
