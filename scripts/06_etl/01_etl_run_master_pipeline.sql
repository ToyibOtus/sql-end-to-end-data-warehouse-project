/*
===========================================================================================================
Stored Procedure: Full ETL Operation (csv file -> staging -> bronze -> silver ->)
===========================================================================================================
Script Purpose:
	This script performs a full etl run, loading all layers from staging, to bronze and silver.
	Additionally, it loads the etl master log table with vital execution details. 
	
Parameter: None

Usage: EXEC etl.run_master_pipeline;

Note:
	Running this script assigns a similar run_id across all layers, and tables. 
	This allows for unified tracking, and thus enabling easy traceability and debugging.
===========================================================================================================
*/
CREATE OR ALTER PROCEDURE etl.run_master_pipeline AS
BEGIN
	-- Declare and map values to variables where necessary
	DECLARE 
	@run_id UNIQUEIDENTIFIER = NEWID(),
	@pipeline_name NVARCHAR(50) = 'etl.run_master_pipeline',
	@start_time DATETIME,
	@end_time DATETIME,
	@load_duration INT,
	@load_status NVARCHAR(50);

	BEGIN TRY
		-- Capture full etl run start time
		SET @start_time = GETDATE();

		-- Load staging layer
		EXEC staging.load_staging_archive_customers @run_id;
		EXEC staging.load_staging_archive_events @run_id;
		EXEC staging.load_staging_archive_order_items @run_id;
		EXEC staging.load_staging_archive_orders @run_id;
		EXEC staging.load_staging_archive_products @run_id;
		EXEC staging.load_staging_archive_reviews @run_id;
		EXEC staging.load_staging_archive_sessions @run_id;

		-- Load bronze layer
		EXEC bronze.load_bronze_archive_customers;
		EXEC bronze.load_bronze_archive_events;
		EXEC bronze.load_bronze_archive_order_items;
		EXEC bronze.load_bronze_archive_orders;
		EXEC bronze.load_bronze_archive_products;
		EXEC bronze.load_bronze_archive_reviews;
		EXEC bronze.load_bronze_archive_sessions;

		-- Load silver layer
		EXEC silver.load_silver_archive_customers;
		EXEC silver.load_silver_archive_events;
		EXEC silver.load_silver_archive_order_items;
		EXEC silver.load_silver_archive_orders;
		EXEC silver.load_silver_archive_products;
		EXEC silver.load_silver_archive_reviews;
		EXEC silver.load_silver_archive_sessions;

		-- Load gold layer
		EXEC gold.load_gold_dim_customers;
		EXEC gold.load_gold_dim_products;
		EXEC gold.load_gold_dim_sessions;
		EXEC gold.load_gold_fact_orders;
		EXEC gold.load_gold_fact_order_items;
		EXEC gold.load_gold_fact_events;
		EXEC gold.load_gold_fact_reviews;

		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);
		SET @load_status = 'Success';

		-- Load etl master log table
		INSERT INTO audit.etl_master_log
		(
			etl_run_id,
			etl_pipeline_name,
			etl_start_time,
			etl_end_time,
			etl_load_duration_seconds,
			etl_load_status
		)
		VALUES
		(
			@run_id,
			@pipeline_name,
			@start_time,
			@end_time,
			@load_duration,
			@load_status
		);
	END TRY

	BEGIN CATCH
		-- Map values to variables
		SET @end_time = GETDATE();
		SET @load_duration = DATEDIFF(second, @start_time, @end_time);
		SET @load_status = 'Failed'

		-- Load etl master log table with error details
		INSERT INTO audit.etl_master_log
		(
			etl_run_id,
			etl_pipeline_name,
			etl_start_time,
			etl_end_time,
			etl_load_duration_seconds,
			etl_load_status,
			etl_error_number,
			etl_error_message,
			etl_error_state,
			etl_error_line,
			etl_error_severity,
			etl_error_procedure
		)
		VALUES
		(
			@run_id,
			@pipeline_name,
			@start_time,
			@end_time,
			@load_duration,
			@load_status,
			ERROR_NUMBER(),
			ERROR_MESSAGE(),
			ERROR_STATE(),
			ERROR_LINE(),
			ERROR_SEVERITY(),
			ERROR_PROCEDURE()
		);
	END CATCH;
END;
