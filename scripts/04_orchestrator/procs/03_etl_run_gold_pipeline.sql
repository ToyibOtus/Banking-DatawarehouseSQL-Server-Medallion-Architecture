/*
===================================================================================
Script    : 03_etl_run_gold_pipeline
Location  : scripts/04_orchestrator/procs/
Author    : Otusanya Toyib Oluwatimilehin
Created   : 2026-04-04
Version   : 1.0
===================================================================================
Script Purpose:
    It loads the gold layer with data from the silver layer. 

Tables Loaded:
	gold.dim_customers
	gold.dim_employees
	gold.dim_branches
	gold.dim_accounts
	gold.fact_transactions
	gold.fact_loan_applications

Usage: EXEC etl.run_gold_pipeline
	
Parameter: None

===================================================================================
Change Log:
	 
	| Version |     Date    |  Description                                     |
	|---------|-------------|--------------------------------------------------|
	|   1.0   |  2026-04-04 |  Initial creation                                |
===================================================================================
*/
USE BankingDW;
GO

CREATE OR ALTER PROCEDURE etl.run_gold_pipeline AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================

	-- Batch-Level variables
	DECLARE
	@batch_id INT,
	@batch_name NVARCHAR(50) = 'etl.run_gold_pipeline',
	@source_system NVARCHAR(50) ='ALL',
	@layer NVARCHAR(50) = 'Gold',
	@start_time DATETIME2,
	@end_time DATETIME2,
	@duration_seconds INT,
	@batch_status NVARCHAR(50) = 'Running',
	@total_rows INT = 0,
	@executed_by NVARCHAR(100) = SUSER_NAME();


	-- =======================================================================================
	-- SECTION 2: OPEN BATCH — Log the start of this pipeline run
	-- =======================================================================================

	-- Retrieve batch start time
	SET @start_time = SYSDATETIME();

	-- Load log details at batch-level
	INSERT INTO etl.batch_log
	(
		batch_name,
		source_system,
		layer,
		start_time,
		load_status,
		total_rows_processed,
		executed_by
	)
	VALUES
	(
		@batch_name,
		@source_system,
		@layer,
		@start_time,
		@batch_status,
		@total_rows,
		@executed_by
	);
	-- Retrieve recently generated batch_id
	SET @batch_id = SCOPE_IDENTITY();

	-- =======================================================================================
	-- SECTION 3: LOAD ALL GOLD TABLES
	-- =======================================================================================
	BEGIN TRY
		EXEC etl.load_gold_crm;
		EXEC etl.load_gold_hrms;
		EXEC etl.load_gold_cbs;
		EXEC etl.load_gold_los;
		EXEC etl.reconcile_gold_keys;

		-- Retrieve total rows processed
		SELECT @total_rows = SUM(COALESCE(total_rows_processed, 0)) FROM etl.batch_log WHERE 
		batch_name IN ('etl.load_gold_crm', 'etl.load_gold_hrms', 'etl.load_gold_cbs', 'etl.load_gold_los') AND
		load_status = 'Success' AND start_time >= @start_time;

		-- Map values to variables on success
		SET @end_time = SYSDATETIME();
		SET @duration_seconds = DATEDIFF(second, @start_time, @end_time);
		SET @batch_status = 'Success';

		-- Update log details at batch level on success
		UPDATE etl.batch_log
			SET
				end_time = @end_time,
				load_duration_seconds = @duration_seconds,
				load_status = @batch_status,
				total_rows_processed = @total_rows
			WHERE batch_id = @batch_id;
	END TRY


	-- =======================================================================================
	-- SECTION 4: ERROR HANDLING
	-- =======================================================================================
	BEGIN CATCH
		-- Map values to variables on failure
		SET @end_time = SYSDATETIME();
		SET @duration_seconds = DATEDIFF(second, @start_time, @end_time);
		SET @batch_status = 'Failed';

		-- Retrieve total rows processed
		SELECT @total_rows = SUM(COALESCE(total_rows_processed, 0)) FROM etl.batch_log WHERE 
		(batch_name IN ('etl.load_gold_crm', 'etl.load_gold_hrms', 'etl.load_gold_cbs', 'etl.load_gold_los') 
		AND start_time >= @start_time);

		-- Update log details at batch level on failure
		UPDATE etl.batch_log
			SET
				end_time = @end_time,
				load_duration_seconds = @duration_seconds,
				load_status = @batch_status,
				total_rows_processed = @total_rows,
				err_message = ERROR_MESSAGE()
			WHERE batch_id = @batch_id;

		THROW;
	END CATCH;
END;
